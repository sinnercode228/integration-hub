"""Domain model: normalized events, stored event records and delivery records."""

from __future__ import annotations

import secrets
import time
from collections.abc import Iterable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


def utcnow() -> datetime:
    return datetime.now(UTC)


def new_id(prefix: str) -> str:
    """Time-sortable, URL-safe id such as ``evt_0192f3c4a1b2c3d4e5f60718``.

    The first 12 hex chars are milliseconds since epoch, so ids sort by creation time
    (handy for SQLite indexes and for humans reading logs).
    """
    return f"{prefix}_{int(time.time() * 1000):012x}{secrets.token_hex(6)}"


class Contact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    phone: str | None = None
    email: str | None = None

    def is_empty(self) -> bool:
        return not (self.name or self.phone or self.email)


class NormalizedEvent(BaseModel):
    """What every inbound connector produces, regardless of the source system."""

    type: str
    external_id: str | None = None
    occurred_at: datetime | None = None
    contact: Contact = Field(default_factory=Contact)
    fields: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    RETRYING = "retrying"
    DELIVERED = "delivered"
    DEAD = "dead"


class EventStatus(StrEnum):
    PROCESSING = "processing"
    DELIVERED = "delivered"
    PARTIAL = "partial"
    FAILED = "failed"
    NO_ROUTE = "no_route"


class Attempt(BaseModel):
    number: int
    started_at: datetime
    duration_ms: float
    ok: bool
    status_code: int | None = None
    error: str | None = None
    retry_in_seconds: float | None = None


class DeliveryRecord(BaseModel):
    id: str
    event_id: str
    route: str
    destination: str
    connector: str
    status: DeliveryStatus = DeliveryStatus.PENDING
    payload: Any = None
    result: dict[str, Any] | None = None
    attempts: list[Attempt] = Field(default_factory=list)
    attempt_base: int = 0
    replays: int = 0
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    @property
    def attempts_in_cycle(self) -> int:
        """Attempts made since creation or since the last manual replay."""
        return len(self.attempts) - self.attempt_base

    @property
    def is_final(self) -> bool:
        return self.status in (DeliveryStatus.DELIVERED, DeliveryStatus.DEAD)


class EventRecord(BaseModel):
    id: str
    source: str
    connector: str
    type: str
    external_id: str | None = None
    idempotency_key: str
    received_at: datetime = Field(default_factory=utcnow)
    status: EventStatus = EventStatus.PROCESSING
    contact: Contact = Field(default_factory=Contact)
    fields: dict[str, Any] = Field(default_factory=dict)
    raw: dict[str, Any] = Field(default_factory=dict)
    deliveries: list[DeliveryRecord] = Field(default_factory=list)

    def template_context(self) -> dict[str, Any]:
        """Variables available to mapping templates (``{{ contact.phone | phone }}``)."""
        event = self.model_dump(mode="json", exclude={"deliveries"})
        return {
            "id": self.id,
            "source": self.source,
            "connector": self.connector,
            "type": self.type,
            "external_id": self.external_id,
            "received_at": self.received_at,
            "contact": self.contact.model_dump(),
            "fields": self.fields,
            "raw": self.raw,
            "event": event,
        }


def aggregate_status(deliveries: Iterable[DeliveryRecord]) -> EventStatus:
    statuses = [d.status for d in deliveries]
    if not statuses:
        return EventStatus.NO_ROUTE
    if any(s in (DeliveryStatus.PENDING, DeliveryStatus.RETRYING) for s in statuses):
        return EventStatus.PROCESSING
    dead = sum(s is DeliveryStatus.DEAD for s in statuses)
    if dead == 0:
        return EventStatus.DELIVERED
    if dead == len(statuses):
        return EventStatus.FAILED
    return EventStatus.PARTIAL
