"""Ingestion pipeline: verify -> parse/normalize -> deduplicate -> store -> route -> enqueue.

The HTTP handler only waits for this fast path; the actual deliveries happen in the worker,
so senders (amoCRM, Bitrix24, Tilda) always get a quick ``202``.
"""

from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

from relay.config import RelayConfig
from relay.connectors.base import InboundConnector, InboundRequest
from relay.domain import DeliveryRecord, EventRecord, aggregate_status, new_id
from relay.errors import PayloadError, SignatureError, UnknownSourceError
from relay.idempotency import IdempotencyStore, derive_key
from relay.log import get_logger
from relay.metrics import RelayMetrics
from relay.queue.base import DeliveryQueue, Job
from relay.routing import plan
from relay.store.base import EventStore

log = get_logger(__name__)


@dataclass(slots=True)
class IngestResult:
    accepted: list[str] = field(default_factory=list)
    duplicates: list[str] = field(default_factory=list)
    ping: bool = False


class IngestService:
    def __init__(
        self,
        *,
        config: RelayConfig,
        inbound: Mapping[str, InboundConnector],
        store: EventStore,
        queue: DeliveryQueue,
        idempotency: IdempotencyStore,
        metrics: RelayMetrics,
        idempotency_ttl: int,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.config = config
        self.inbound = inbound
        self.store = store
        self.queue = queue
        self.idempotency = idempotency
        self.metrics = metrics
        self.idempotency_ttl = idempotency_ttl
        self.clock = clock

    async def ingest(
        self, source_id: str, request: InboundRequest, *, idempotency_key: str | None = None
    ) -> IngestResult:
        connector = self.inbound.get(source_id)
        source = self.config.sources.get(source_id)
        if connector is None or source is None or not source.enabled:
            raise UnknownSourceError(source_id)
        counter = self.metrics.events_received

        try:
            connector.verify(request, now=self.clock())
        except SignatureError:
            counter.labels(source_id, "rejected_signature").inc()
            log.warning("webhook.signature_rejected", source=source_id)
            raise
        try:
            parsed = await connector.parse(request)
        except PayloadError:
            counter.labels(source_id, "invalid_payload").inc()
            raise
        if parsed.ping:
            counter.labels(source_id, "ping").inc()
            return IngestResult(ping=True)

        result = IngestResult()
        total = len(parsed.events)
        for index, normalized in enumerate(parsed.events):
            key = derive_key(
                source_id,
                normalized,
                header_key=idempotency_key,
                body=request.body,
                index=index,
                total=total,
            )
            event_id = new_id("evt")
            existing = await self.idempotency.claim(key, event_id, self.idempotency_ttl)
            if existing is not None:
                counter.labels(source_id, "duplicate").inc()
                result.duplicates.append(existing)
                log.info("webhook.duplicate", source=source_id, event_id=existing, key=key)
                continue

            record = EventRecord(
                id=event_id,
                source=source_id,
                connector=connector.kind,
                type=normalized.type,
                external_id=normalized.external_id,
                idempotency_key=key,
                received_at=request.received_at,
                contact=normalized.contact,
                fields=normalized.fields,
                raw=normalized.raw,
            )
            context = record.template_context()
            for route, target in plan(self.config, source_id, context):
                destination = self.config.destinations[target.to]
                record.deliveries.append(
                    DeliveryRecord(
                        id=new_id("dlv"),
                        event_id=event_id,
                        route=route.name,
                        destination=target.to,
                        connector=destination.connector,
                    )
                )
            record.status = aggregate_status(record.deliveries)
            try:
                await self.store.add_event(record)
            except Exception:
                await self.idempotency.release(key)
                raise
            for delivery in record.deliveries:
                await self.queue.enqueue(Job(delivery_id=delivery.id, event_id=event_id))

            counter.labels(source_id, "accepted").inc()
            result.accepted.append(event_id)
            log.info(
                "webhook.accepted",
                source=source_id,
                event_id=event_id,
                type=record.type,
                deliveries=len(record.deliveries),
            )
        return result
