"""Queue, idempotency and store backends - every implementation passes the same contract."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import timedelta
from pathlib import Path
from typing import Any

import fakeredis
import pytest

from relay.domain import (
    Contact,
    DeliveryRecord,
    DeliveryStatus,
    EventRecord,
    EventStatus,
    new_id,
    utcnow,
)
from relay.idempotency import InMemoryIdempotencyStore, RedisIdempotencyStore
from relay.queue import InMemoryQueue, RedisQueue
from relay.queue.base import Job
from relay.store import InMemoryEventStore, SqliteEventStore
from relay.store.base import EventFilter

from .conftest import FakeClock


@pytest.fixture(params=["memory", "redis"])
async def queue(request: pytest.FixtureRequest, clock: FakeClock) -> AsyncIterator[Any]:
    if request.param == "memory":
        yield InMemoryQueue(clock=clock)
    else:
        redis = fakeredis.FakeAsyncRedis()
        q = RedisQueue(redis, prefix="t", clock=clock)
        yield q
        await q.close()


class TestQueue:
    async def test_delay_reserve_ack(self, queue: Any, clock: FakeClock) -> None:
        a, b = Job("d1", "e1"), Job("d2", "e1")
        await queue.enqueue(a)
        await queue.enqueue(b, delay=10)
        assert await queue.reserve(limit=10, lease_seconds=30) == [a]
        depth = await queue.depth()
        assert (depth.ready, depth.scheduled, depth.in_flight) == (0, 1, 1)
        await queue.ack(a)
        clock.advance(10)
        assert await queue.reserve(limit=10, lease_seconds=30) == [b]
        assert await queue.reserve(limit=10, lease_seconds=30) == []

    async def test_expired_lease_is_requeued(self, queue: Any, clock: FakeClock) -> None:
        job = Job("d1", "e1")
        await queue.enqueue(job)
        await queue.reserve(limit=1, lease_seconds=5)
        assert await queue.requeue_expired() == 0
        clock.advance(6)
        assert await queue.requeue_expired() == 1
        assert await queue.reserve(limit=1, lease_seconds=5) == [job]

    async def test_enqueue_twice_only_moves_due_time(self, queue: Any) -> None:
        job = Job("d1", "e1")
        await queue.enqueue(job, delay=100)
        await queue.enqueue(job)
        assert await queue.reserve(limit=5, lease_seconds=5) == [job]

    async def test_reserve_returns_the_lease_as_a_token(self, queue: Any, clock: FakeClock) -> None:
        await queue.enqueue(Job("d1", "e1"))
        (job,) = await queue.reserve(limit=1, lease_seconds=30)
        assert job.lease == clock() + 30
        assert job == Job("d1", "e1")  # the token is not part of the job's identity

    async def test_extend_moves_the_deadline(self, queue: Any, clock: FakeClock) -> None:
        await queue.enqueue(Job("d1", "e1"))
        (job,) = await queue.reserve(limit=1, lease_seconds=5)
        clock.advance(4)
        renewed = await queue.extend(job, lease_seconds=5)
        assert renewed is not None and renewed.lease == clock() + 5
        clock.advance(4)  # past the first deadline, inside the renewed one
        assert await queue.requeue_expired() == 0
        assert await queue.ack(job) is False  # the old token no longer matches
        assert await queue.ack(renewed) is True
        assert (await queue.depth()).in_flight == 0

    async def test_retry_moves_the_job_in_one_step(self, queue: Any, clock: FakeClock) -> None:
        await queue.enqueue(Job("d1", "e1"))
        (job,) = await queue.reserve(limit=1, lease_seconds=30)
        assert await queue.retry(job, delay=10) is True
        depth = await queue.depth()
        assert (depth.ready, depth.scheduled, depth.in_flight) == (0, 1, 0)
        clock.advance(10)
        assert await queue.reserve(limit=1, lease_seconds=30) == [job]

    async def test_bury_parks_the_job(self, queue: Any) -> None:
        await queue.enqueue(Job("d1", "e1"))
        (job,) = await queue.reserve(limit=1, lease_seconds=30)
        assert await queue.bury(job, "boom") is True
        depth = await queue.depth()
        assert (depth.in_flight, depth.dead) == (0, 1)
        (parked,) = await queue.dead_letters()
        assert (parked.job, parked.reason) == (job, "boom")

    async def test_a_job_without_a_token_is_not_fenced(self, queue: Any) -> None:
        job = Job("d1", "e1")
        await queue.enqueue(job)
        await queue.reserve(limit=1, lease_seconds=30)
        assert await queue.extend(job, lease_seconds=30) == job
        assert await queue.ack(job) is True
        assert (await queue.depth()).in_flight == 0

    async def test_enqueue_missing_adds_only_absent_jobs(
        self, queue: Any, clock: FakeClock
    ) -> None:
        later, held, lost = Job("d1", "e1"), Job("d2", "e1"), Job("d3", "e1")
        await queue.enqueue(held)
        (leased,) = await queue.reserve(limit=1, lease_seconds=30)
        await queue.enqueue(later, delay=60)
        assert await queue.enqueue_missing([]) == []
        assert await queue.enqueue_missing([later, held, lost, lost]) == [lost]
        depth = await queue.depth()
        assert (depth.ready, depth.scheduled, depth.in_flight) == (1, 1, 1)
        assert await queue.reserve(limit=5, lease_seconds=30) == [lost]
        clock.advance(59)
        assert await queue.reserve(limit=5, lease_seconds=30) == []  # due time not moved
        assert await queue.ack(leased) is True  # the lease is untouched

    async def test_dead_letters(self, queue: Any, clock: FakeClock) -> None:
        await queue.dead_letter(Job("d1", "e1"), "boom")
        clock.advance(1)
        await queue.dead_letter(Job("d2", "e1"), "bang")
        items = await queue.dead_letters()
        assert [i.job.delivery_id for i in items] == ["d2", "d1"]
        assert items[0].reason == "bang"
        await queue.remove_dead("d2")
        assert (await queue.depth()).dead == 1
        assert await queue.ping()


@pytest.mark.parametrize("backend", ["memory", "redis"])
async def test_idempotency_claim(backend: str, clock: FakeClock) -> None:
    store: Any = (
        InMemoryIdempotencyStore(clock=clock)
        if backend == "memory"
        else RedisIdempotencyStore(fakeredis.FakeAsyncRedis(), prefix="t")
    )
    assert await store.claim("k", "evt_1", 60) is None
    assert await store.claim("k", "evt_2", 60) == "evt_1"
    await store.release("k")
    assert await store.claim("k", "evt_3", 60) is None


async def test_memory_idempotency_expires(clock: FakeClock) -> None:
    store = InMemoryIdempotencyStore(clock=clock)
    await store.claim("k", "a", 10)
    clock.advance(11)
    assert await store.claim("k", "b", 10) is None


def make_event(source: str = "site", name: str = "Анна", n_deliveries: int = 2) -> EventRecord:
    event_id = new_id("evt")
    return EventRecord(
        id=event_id,
        source=source,
        connector="tilda",
        type="form.submitted",
        external_id=f"ext-{event_id[-4:]}",
        idempotency_key=f"k-{event_id}",
        contact=Contact(name=name, phone="+79000000000"),
        fields={"comment": "hi"},
        deliveries=[
            DeliveryRecord(
                id=new_id("dlv"),
                event_id=event_id,
                route="r",
                destination=f"dest{i}",
                connector="webhook",
            )
            for i in range(n_deliveries)
        ],
    )


@pytest.fixture(params=["memory", "sqlite"])
async def store(request: pytest.FixtureRequest, tmp_path: Path) -> AsyncIterator[Any]:
    impl: Any = (
        InMemoryEventStore() if request.param == "memory" else SqliteEventStore(tmp_path / "e.db")
    )
    yield impl
    await impl.close()


class TestStore:
    async def test_roundtrip_and_status_aggregation(self, store: Any) -> None:
        event = make_event()
        await store.add_event(event)
        loaded = await store.get_event(event.id)
        assert loaded == event
        first, second = loaded.deliveries
        first.status = DeliveryStatus.DELIVERED
        assert await store.save_delivery(first) is EventStatus.PROCESSING
        second.status = DeliveryStatus.DEAD
        second.updated_at = utcnow() + timedelta(seconds=1)
        assert await store.save_delivery(second) is EventStatus.PARTIAL
        assert (await store.get_event(event.id)).status is EventStatus.PARTIAL
        assert (await store.get_delivery(second.id)).status is DeliveryStatus.DEAD
        dead = await store.list_deliveries(DeliveryStatus.DEAD)
        assert [d.id for d in dead] == [second.id]
        assert len(await store.list_deliveries()) == 2

    async def test_filters_search_pagination(self, store: Any) -> None:
        events = [
            make_event("site", "Анна"),
            make_event("crm", "Boris"),
            make_event("site", "Vera"),
        ]
        for event in events:
            await store.add_event(event)
        items, total = await store.list_events(EventFilter(source="site"))
        assert total == 2 and {e.contact.name for e in items} == {"Анна", "Vera"}
        items, total = await store.list_events(EventFilter(query="bor"))
        assert total == 1 and items[0].source == "crm"
        items, total = await store.list_events(EventFilter(query="100%_"))
        assert total == 0
        items, total = await store.list_events(EventFilter(limit=1, offset=1))
        assert total == 3 and len(items) == 1 and items[0].deliveries
        items, _ = await store.list_events(EventFilter(status=EventStatus.DELIVERED))
        assert items == []

    async def test_overdue_deliveries(self, store: Any) -> None:
        now = utcnow()
        old, fresh = now - timedelta(minutes=10), now - timedelta(seconds=10)
        cutoff = now - timedelta(minutes=2)
        event = make_event(n_deliveries=6)
        pending_old, pending_fresh, waiting, due, delivered, dead = event.deliveries
        for d in event.deliveries:
            d.created_at = d.updated_at = old
        pending_fresh.updated_at = fresh
        waiting.status = due.status = DeliveryStatus.RETRYING
        waiting.next_attempt_at = now + timedelta(hours=1)  # in its backoff
        due.next_attempt_at = old - timedelta(minutes=1)
        delivered.status, dead.status = DeliveryStatus.DELIVERED, DeliveryStatus.DEAD
        await store.add_event(event)

        found = await store.overdue_deliveries(cutoff)
        assert [d.id for d in found] == [due.id, pending_old.id]  # longest overdue first
        assert [d.id for d in await store.overdue_deliveries(cutoff, limit=1)] == [due.id]
        assert await store.overdue_deliveries(old - timedelta(hours=1)) == []

    async def test_stats_and_missing(self, store: Any) -> None:
        await store.add_event(make_event("site"))
        await store.add_event(make_event("crm", n_deliveries=1))
        stats = await store.stats()
        assert stats.events_total == 2
        assert stats.events_by_source == {"site": 1, "crm": 1}
        assert stats.deliveries_by_status == {"pending": 3}
        assert stats.deliveries_by_destination["dest0"] == {"pending": 2}
        assert await store.get_event("nope") is None
        assert await store.get_delivery("nope") is None
        assert await store.ping()
