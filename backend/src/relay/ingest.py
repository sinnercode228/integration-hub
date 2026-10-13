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
from relay.domain import (
    DeliveryRecord,
    DeliveryStatus,
    EventRecord,
    aggregate_status,
    new_id,
    utcnow,
)
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
            queued = 0
            try:
                for delivery in record.deliveries:
                    await self.queue.enqueue(Job(delivery_id=delivery.id, event_id=event_id))
                    queued += 1
            except Exception as exc:
                # A job that got in may already be with a worker, so a retry let in as a new
                # event would send it twice. Then the key stays and the sweep sends the rest.
                released = queued == 0 and await self._abandon(record, key, exc)
                log.exception(
                    "webhook.enqueue_failed",
                    source=source_id,
                    event_id=event_id,
                    queued=queued,
                    released=released,
                )
                raise

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

    async def _abandon(self, record: EventRecord, key: str, exc: Exception) -> bool:
        """Undo an intake none of whose jobs reached the queue; True if the key was released.

        The sender gets ``500`` and retries, so the retry has to come in as a new event and
        this one must not be delivered as well. Its deliveries are marked dead first and the
        key is released after that.

        If the key cannot be released, the retry will be answered as a duplicate. Then the
        deliveries go back to pending, and the worker's sweep queues them once they are a
        lease overdue. If marking them fails, the key is kept too: the deliveries not marked
        yet stay pending for the sweep, the ones already marked can be replayed.
        """
        error = (
            f"not queued at intake ({type(exc).__name__}); "
            "the sender got 500, so a resend comes in as a new event"
        )
        try:
            for delivery in record.deliveries:
                failed = delivery.model_copy(
                    update={"status": DeliveryStatus.DEAD, "last_error": error}
                )
                failed.updated_at = utcnow()
                await self.store.save_delivery(failed)
        except Exception:
            log.exception("webhook.abandon_failed", event_id=record.id, stage="mark")
            return False
        try:
            await self.idempotency.release(key)
        except Exception:
            log.exception("webhook.abandon_failed", event_id=record.id, stage="release")
            try:
                for delivery in record.deliveries:
                    delivery.updated_at = utcnow()
                    await self.store.save_delivery(delivery)
            except Exception:
                log.exception("webhook.abandon_failed", event_id=record.id, stage="restore")
            return False
        return True
