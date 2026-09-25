"""Event store interface: persistence for events, deliveries and attempt history."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from relay.domain import DeliveryRecord, DeliveryStatus, EventRecord, EventStatus


@dataclass(frozen=True, slots=True)
class EventFilter:
    status: EventStatus | None = None
    source: str | None = None
    type: str | None = None
    query: str | None = None
    limit: int = 50
    offset: int = 0


@dataclass(slots=True)
class StoreStats:
    events_total: int = 0
    events_by_status: dict[str, int] = field(default_factory=dict)
    events_by_source: dict[str, int] = field(default_factory=dict)
    deliveries_by_status: dict[str, int] = field(default_factory=dict)
    deliveries_by_destination: dict[str, dict[str, int]] = field(default_factory=dict)


class EventStore(Protocol):
    async def add_event(self, event: EventRecord) -> None: ...

    async def get_event(self, event_id: str) -> EventRecord | None: ...

    async def list_events(self, flt: EventFilter) -> tuple[list[EventRecord], int]: ...

    async def get_delivery(self, delivery_id: str) -> DeliveryRecord | None: ...

    async def save_delivery(self, delivery: DeliveryRecord) -> EventStatus:
        """Persist a delivery and return the recomputed aggregate status of its event."""
        ...

    async def list_deliveries(
        self, status: DeliveryStatus | None = None, limit: int = 100
    ) -> list[DeliveryRecord]: ...

    async def stats(self) -> StoreStats: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...


def matches_query(event: EventRecord, query: str) -> bool:
    needle = query.lower()
    haystack = [
        event.id,
        event.external_id or "",
        event.type,
        event.contact.name or "",
        event.contact.phone or "",
        event.contact.email or "",
    ]
    return any(needle in item.lower() for item in haystack)
