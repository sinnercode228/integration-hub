"""Several worker processes on one Redis.

Each test lets another worker run right after this worker's first Redis round trip. A queue
that reads members in one round trip and claims them in the next gives the other worker a
window between the two. An atomic claim has no such window: whatever runs in between sees
either the state before the claim or the state after it.

The tests run on fakeredis, which executes the Lua scripts through lupa. Set
``RELAY_TEST_REDIS_URL`` (CI does) to run them against a real Redis server as well.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Any

import fakeredis
import pytest
from redis.asyncio import Redis

from relay.queue import InMemoryQueue, RedisQueue
from relay.queue.base import Job

from .conftest import FakeClock

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


Workers = Callable[[int], tuple[list[Any], list[RedisQueue]]]


@pytest.fixture
async def workers(clock: FakeClock) -> AsyncIterator[Workers]:
    """Makes ``n`` queues on separate connections to one Redis server."""
    url = os.environ.get("RELAY_TEST_REDIS_URL")
    server = fakeredis.FakeServer()
    prefix = f"race-{uuid.uuid4().hex}"
    clients: list[Any] = []

    def make(n: int) -> tuple[list[Any], list[RedisQueue]]:
        new = [
            Redis.from_url(url) if url else fakeredis.FakeAsyncRedis(server=server)
            for _ in range(n)
        ]
        clients.extend(new)
        return new, [RedisQueue(c, prefix=prefix, clock=clock) for c in new]

    yield make
    if clients:
        await clients[0].delete(*(f"{prefix}:q:{k}" for k in ("scheduled", "inflight", "dead")))
    for client in clients:
        await client.aclose()


async def test_a_retry_is_not_claimed_before_its_backoff(workers: Workers) -> None:
    clients, (a, b) = workers(2)
    job = Job("d1", "e1")
    await a.enqueue(job)
    claims: list[str] = []

    async def worker_a_fails_once() -> None:
        taken = await a.reserve(limit=1, lease_seconds=LEASE)
        claims.extend("a" for _ in taken)
        for j in taken:
            await a.enqueue(j, delay=2)
            await a.ack(j)

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


@pytest.mark.parametrize("backend", ["memory", "redis"])
@pytest.mark.xfail(
    strict=True,
    reason="ack is not fenced by the lease: a worker whose lease ran out removes the lease "
    "of the worker that took the job over",
)
async def test_a_late_ack_does_not_drop_the_new_owners_lease(
    backend: str, clock: FakeClock
) -> None:
    queue: Any = (
        InMemoryQueue(clock=clock)
        if backend == "memory"
        else RedisQueue(fakeredis.FakeAsyncRedis(), prefix="t", clock=clock)
    )
    job = Job("d1", "e1")
    await queue.enqueue(job)
    await queue.reserve(limit=1, lease_seconds=5)  # worker x
    clock.advance(6)  # x is stuck on a slow destination, its lease runs out
    assert await queue.requeue_expired() == 1
    assert await queue.reserve(limit=1, lease_seconds=5) == [job]  # worker y takes over
    await queue.ack(job)  # x finally finishes and acks
    clock.advance(6)  # y dies without acking
    assert await queue.requeue_expired() == 1, "the job should come back after y's lease"
