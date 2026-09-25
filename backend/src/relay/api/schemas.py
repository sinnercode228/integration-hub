"""Response models of the admin API (mirrored by ``dashboard/src/api/types.ts``)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel

from relay.domain import Contact, DeliveryRecord, DeliveryStatus, EventRecord, EventStatus


class DeliveryBrief(BaseModel):
    id: str
    route: str
    destination: str
    connector: str
    status: DeliveryStatus
    attempts: int
    last_error: str | None
    next_attempt_at: datetime | None
    updated_at: datetime

    @classmethod
    def of(cls, d: DeliveryRecord) -> DeliveryBrief:
        return cls(
            id=d.id,
            route=d.route,
            destination=d.destination,
            connector=d.connector,
            status=d.status,
            attempts=len(d.attempts),
            last_error=d.last_error,
            next_attempt_at=d.next_attempt_at,
            updated_at=d.updated_at,
        )


class EventSummary(BaseModel):
    id: str
    source: str
    connector: str
    type: str
    status: EventStatus
    external_id: str | None
    received_at: datetime
    contact: Contact
    deliveries: list[DeliveryBrief]

    @classmethod
    def of(cls, e: EventRecord) -> EventSummary:
        return cls(
            id=e.id,
            source=e.source,
            connector=e.connector,
            type=e.type,
            status=e.status,
            external_id=e.external_id,
            received_at=e.received_at,
            contact=e.contact,
            deliveries=[DeliveryBrief.of(d) for d in e.deliveries],
        )


class EventPage(BaseModel):
    items: list[EventSummary]
    total: int
    limit: int
    offset: int


class QueueInfo(BaseModel):
    ready: int
    scheduled: int
    in_flight: int
    dead: int


class StatsResponse(BaseModel):
    events_total: int
    events_by_status: dict[str, int]
    events_by_source: dict[str, int]
    deliveries_by_status: dict[str, int]
    deliveries_by_destination: dict[str, dict[str, int]]
    queue: QueueInfo


class DeadLetterItem(BaseModel):
    delivery: DeliveryRecord
    event: EventSummary | None


class ReplayResponse(BaseModel):
    replayed: list[str]
    skipped: list[str]


class ConnectorsResponse(BaseModel):
    sources: list[dict[str, Any]]
    destinations: list[dict[str, Any]]
    routes: list[dict[str, Any]]
    stock: dict[str, Any] | None
    available: dict[str, list[dict[str, str]]]
    retry_schedule_seconds: list[float]


class StockItem(BaseModel):
    sku: str
    available: int
    status: str


class StockResponse(BaseModel):
    items: list[StockItem]
    stale: bool
