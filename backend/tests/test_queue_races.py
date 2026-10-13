"""Several worker processes on one Redis.

Each test lets another worker run right after this worker's first Redis round trip. A queue
that reads members in one round trip and claims them in the next gives the other worker a
window between the two. An atomic claim has no such window: whatever runs in between sees
either the state before the claim or the state after it.

The last group covers a worker whose lease ran out while another worker took the job over:
every operation it tries with its old token has to be refused. The ones after it cover a
renewal whose reply was lost on the way back.

The tests run on fakeredis, which executes the Lua scripts through lupa. Set
``RELAY_TEST_REDIS_URL`` (CI does) to run them against a real Redis server as well.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest
import redis.exceptions

from relay.queue import InMemoryQueue
from relay.queue.base import Job

from .conftest import FakeClock, LoseNextReply, Workers

LEASE = 120.0


class AfterFirstRoundTrip:
    """Runs ``action`` once, right after the client's next command returns."""

    def __init__(self, client: Any, action: Callable[[], Awaitable[None]]) -> None:
        self._execute = client.execute_command
        self._action = action
        self.fired = False
        client.execute_command = self._wrapped

    async def _wrapped(self, *args: Any, **kwargs: Any) -> Any:
        result = await self._execute(*args, **kwargs)
        if not self.fired:
            self.fired = True
            await self._action()
        return result


async def test_a_retry_is_not_claimed_before_its_backoff(workers: Workers) -> None:
    clients, (a, b) = workers(2)
    job = Job("d1", "e1")
    await a.enqueue(job)
    claims: list[str] = []

    async def worker_a_fails_once() -> None:
        taken = await a.reserve(limit=1, lease_seconds=LEASE)
        claims.extend("a" for _ in taken)
        for j in taken:
            await a.retry(j, delay=2)

    hook = AfterFirstRoundTrip(clients[1], worker_a_fails_once)
    claims.extend("b" for _ in await b.reserve(limit=1, lease_seconds=LEASE))

    assert hook.fired
    assert len(claims) == 1, f"one due job, claimed by {claims}"
    depth = await a.depth()
    if claims == ["a"]:
        # A's retry waits out its 2 s backoff in `scheduled`, nobody holds it.
        assert (depth.ready, depth.scheduled, depth.in_flight) == (0, 1, 0)
    else:
        assert (depth.ready, depth.scheduled, depth.in_flight) == (0, 0, 1)


async def test_requeue_does_not_take_back_a_fresh_lease(workers: Workers, clock: FakeClock) -> None:
    clients, (x, b, c, d) = workers(4)
    job = Job("d1", "e1")
    await x.enqueue(job)
    assert await x.reserve(limit=1, lease_seconds=LEASE) == [job]
    clock.advance(LEASE + 1)  # x died holding the job
    taken_by_c: list[Job] = []

    async def worker_c_recovers_and_claims() -> None:
        await c.requeue_expired()
        taken_by_c.extend(await c.reserve(limit=1, lease_seconds=LEASE))

    hook = AfterFirstRoundTrip(clients[1], worker_c_recovers_and_claims)
    await b.requeue_expired()

    assert hook.fired
    assert taken_by_c == [job]
    # c is still sending: nobody else may get the job until c's lease runs out.
    assert await d.reserve(limit=1, lease_seconds=LEASE) == []
    depth = await d.depth()
    assert (depth.ready, depth.scheduled, depth.in_flight) == (0, 0, 1)


async def take_over(queue: Any, clock: FakeClock) -> tuple[Job, Job]:
    """Worker x claims the job, its lease runs out, worker y claims it again."""
    await queue.enqueue(Job("d1", "e1"))
    (x_job,) = await queue.reserve(limit=1, lease_seconds=5)
    clock.advance(6)  # x is stuck on a slow destination
    assert await queue.requeue_expired() == 1
    (y_job,) = await queue.reserve(limit=1, lease_seconds=5)
    return x_job, y_job


@pytest.fixture(params=["memory", "redis"])
def queue(request: pytest.FixtureRequest, clock: FakeClock, workers: Workers) -> Any:
    if request.param == "memory":
        return InMemoryQueue(clock=clock)
    _, (q,) = workers(1)
    return q


async def test_a_late_ack_does_not_drop_the_new_owners_lease(queue: Any, clock: FakeClock) -> None:
    x_job, _ = await take_over(queue, clock)
    assert await queue.ack(x_job) is False  # x finally finishes and acks
    clock.advance(6)  # y dies without acking
    assert await queue.requeue_expired() == 1, "the job should come back after y's lease"


async def test_a_late_retry_does_not_reschedule_the_new_owners_job(
    queue: Any, clock: FakeClock
) -> None:
    x_job, y_job = await take_over(queue, clock)
    assert await queue.retry(x_job, delay=1) is False
    clock.advance(1)
    assert await queue.reserve(limit=1, lease_seconds=5) == [], "only y may hold the job"
    assert await queue.ack(y_job) is True


async def test_a_late_bury_does_not_dead_letter_the_new_owners_job(
    queue: Any, clock: FakeClock
) -> None:
    x_job, y_job = await take_over(queue, clock)
    assert await queue.bury(x_job, "gave up") is False
    depth = await queue.depth()
    assert (depth.in_flight, depth.dead) == (1, 0)
    assert await queue.ack(y_job) is True


async def test_a_late_extend_does_not_steal_the_job_back(queue: Any, clock: FakeClock) -> None:
    x_job, y_job = await take_over(queue, clock)
    assert await queue.extend(x_job, lease_seconds=5) is None
    assert await queue.ack(y_job) is True


# The sweep puts back jobs for deliveries that lost theirs. It must never add a job that is
# scheduled or that a worker holds, or the job would end up in both sets.


async def test_a_sweep_does_not_requeue_a_leased_job(queue: Any, clock: FakeClock) -> None:
    x_job, y_job = await take_over(queue, clock)
    assert await queue.enqueue_missing([x_job]) == []
    assert await queue.reserve(limit=1, lease_seconds=5) == [], "only y may hold the job"
    assert await queue.ack(y_job) is True
    assert await queue.enqueue_missing([x_job]) == [x_job]


async def test_a_sweep_does_not_add_a_job_another_worker_just_claimed(workers: Workers) -> None:
    clients, (a, b) = workers(2)
    job = Job("d1", "e1")
    taken_by_a: list[Job] = []

    async def worker_a_sweeps_and_claims() -> None:
        await a.enqueue_missing([job])
        taken_by_a.extend(await a.reserve(limit=1, lease_seconds=LEASE))

    hook = AfterFirstRoundTrip(clients[1], worker_a_sweeps_and_claims)
    await b.enqueue_missing([job])

    assert hook.fired
    assert taken_by_a == [job]
    # a is sending: b's sweep may not have left a second copy in `scheduled`.
    depth = await a.depth()
    assert (depth.ready, depth.scheduled, depth.in_flight) == (0, 0, 1)


# A renewal whose reply is lost: Redis applied it, but the worker got an error and still holds
# the old token. Its next renewal must not take its own new deadline for another worker's.


async def test_an_extend_whose_reply_was_lost_keeps_the_lease(
    workers: Workers, clock: FakeClock
) -> None:
    clients, (a, b) = workers(2)
    await a.enqueue(Job("d1", "e1"))
    (job,) = await a.reserve(limit=1, lease_seconds=LEASE)
    clock.advance(LEASE / 2)
    LoseNextReply(clients[0])
    with pytest.raises(redis.exceptions.ConnectionError):
        await a.extend(job, lease_seconds=LEASE)
    clock.advance(LEASE / 2 + 1)  # past the lease from reserve, not past the renewed one
    assert await b.requeue_expired() == 0
    assert await b.reserve(limit=1, lease_seconds=LEASE) == []

    held = await a.extend(job, lease_seconds=LEASE)  # still with the token from reserve
    assert held is not None, "the worker took its own renewal for a takeover"
    assert await a.ack(held) is True


async def test_a_lost_reply_does_not_hide_a_takeover(workers: Workers, clock: FakeClock) -> None:
    clients, (a, b) = workers(2)
    await a.enqueue(Job("d1", "e1"))
    (job,) = await a.reserve(limit=1, lease_seconds=LEASE)
    clock.advance(LEASE / 2)
    LoseNextReply(clients[0])
    with pytest.raises(redis.exceptions.ConnectionError):
        await a.extend(job, lease_seconds=LEASE)
    clock.advance(LEASE + 1)  # past the renewed lease too
    assert await b.requeue_expired() == 1
    (taken,) = await b.reserve(limit=1, lease_seconds=LEASE)

    assert await a.extend(job, lease_seconds=LEASE) is None
    assert await b.ack(taken) is True
