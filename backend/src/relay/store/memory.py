"""In-memory event store for tests and throwaway dev runs."""

from __future__ import annotations

import asyncio
from collections import Counter, defaultdict

from relay.domain import DeliveryRecord, DeliveryStatus, EventRecord, EventStatus, aggregate_status
from relay.store.base import EventFilter, StoreStats, matches_query


class InMemoryEventStore:
    def __init__(self) -> None:
        self._events: dict[str, EventRecord] = {}
        self._delivery_index: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def add_event(self, event: EventRecord) -> None:
        async with self._lock:
            stored = event.model_copy(deep=True)
            self._events[stored.id] = stored
            for delivery in stored.deliveries:
                self._delivery_index[delivery.id] = stored.id

    async def get_event(self, event_id: str) -> EventRecord | None:
        event = self._events.get(event_id)
        return event.model_copy(deep=True) if event else None

    async def list_events(self, flt: EventFilter) -> tuple[list[EventRecord], int]:
        items = [
            event
            for event in self._events.values()
            if (flt.status is None or event.status == flt.status)
            and (flt.source is None or event.source == flt.source)
            and (flt.type is None or event.type == flt.type)
            and (not flt.query or matches_query(event, flt.query))
        ]
        items.sort(key=lambda e: (e.received_at, e.id), reverse=True)
        page = items[flt.offset : flt.offset + flt.limit]
        return [event.model_copy(deep=True) for event in page], len(items)

    async def get_delivery(self, delivery_id: str) -> DeliveryRecord | None:
        event_id = self._delivery_index.get(delivery_id)
        if event_id is None:
            return None
        for delivery in self._events[event_id].deliveries:
            if delivery.id == delivery_id:
                return delivery.model_copy(deep=True)
        return None

    async def save_delivery(self, delivery: DeliveryRecord) -> EventStatus:
        async with self._lock:
            event = self._events[delivery.event_id]
            event.deliveries = [
                delivery.model_copy(deep=True) if d.id == delivery.id else d
                for d in event.deliveries
            ]
            event.status = aggregate_status(event.deliveries)
            return event.status

    async def list_deliveries(
        self, status: DeliveryStatus | None = None, limit: int = 100
    ) -> list[DeliveryRecord]:
        found = [
            d.model_copy(deep=True)
            for event in self._events.values()
            for d in event.deliveries
            if status is None or d.status == status
        ]
        found.sort(key=lambda d: d.updated_at, reverse=True)
        return found[:limit]

    async def stats(self) -> StoreStats:
        stats = StoreStats(events_total=len(self._events))
        stats.events_by_status = dict(Counter(e.status.value for e in self._events.values()))
        stats.events_by_source = dict(Counter(e.source for e in self._events.values()))
        deliveries = [d for e in self._events.values() for d in e.deliveries]
        stats.deliveries_by_status = dict(Counter(d.status.value for d in deliveries))
        per_destination: dict[str, Counter[str]] = defaultdict(Counter)
        for d in deliveries:
            per_destination[d.destination][d.status.value] += 1
        stats.deliveries_by_destination = {k: dict(v) for k, v in per_destination.items()}
        return stats

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return None
