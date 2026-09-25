"""Delivery worker: retries with exponential backoff + jitter, dead-letter, manual replay."""

from __future__ import annotations

import asyncio
import contextlib
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from relay.config import RelayConfig
from relay.connectors.base import DeliveryContext, OutboundConnector
from relay.domain import Attempt, DeliveryRecord, DeliveryStatus, EventRecord, utcnow
from relay.errors import DeliveryError, TemplateError
from relay.log import get_logger
from relay.metrics import RelayMetrics
from relay.queue.base import DeliveryQueue, Job
from relay.security import redact
from relay.store.base import EventStore
from relay.templating import render_payload

log = get_logger(__name__)


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """``delay(n) = min(max_delay, base * factor ** (n - 1))``, minus up to ``jitter`` share.

    With the defaults (2s, x2, 8 attempts) a delivery is retried after roughly
    2s, 4s, 8s, 16s, 32s, 64s, 128s and then dead-lettered - about 4 minutes of total
    outage tolerance; production configs typically raise ``max_attempts`` to cover hours.
    Jitter spreads retries so that a recovered upstream is not hit by a thundering herd.
    """

    max_attempts: int = 8
    base_delay: float = 2.0
    factor: float = 2.0
    max_delay: float = 3600.0
    jitter: float = 0.2

    def delay(self, attempt: int, rng: random.Random | None = None) -> float:
        raw = min(self.max_delay, self.base_delay * self.factor ** max(0, attempt - 1))
        if self.jitter <= 0:
            return raw
        roll = (rng or random).random()
        return raw * (1 - self.jitter * roll)

    def schedule(self) -> list[float]:
        return [self.delay(n, random.Random(0)) for n in range(1, self.max_attempts)]  # noqa: S311


class DeliveryWorker:
    def __init__(
        self,
        *,
        config: RelayConfig,
        store: EventStore,
        queue: DeliveryQueue,
        destinations: Mapping[str, OutboundConnector],
        metrics: RelayMetrics,
        policy: RetryPolicy,
        concurrency: int = 4,
        lease_seconds: float = 120.0,
        default_timeout: float = 30.0,
        clock: Callable[[], float] = time.time,
        rng: random.Random | None = None,
    ) -> None:
        self.config = config
        self.store = store
        self.queue = queue
        self.destinations = destinations
        self.metrics = metrics
        self.policy = policy
        self.concurrency = concurrency
        self.lease_seconds = lease_seconds
        self.default_timeout = default_timeout
        self.clock = clock
        self.rng = rng or random.Random()  # noqa: S311
        self._semaphore = asyncio.Semaphore(concurrency)

    # -- loop -------------------------------------------------------------------------------

    async def run(self, stop: asyncio.Event, poll_interval: float = 0.5) -> None:
        log.info("worker.started", concurrency=self.concurrency)
        while not stop.is_set():
            try:
                processed = await self.process_due()
            except Exception:
                log.exception("worker.iteration_failed")
                processed = 0
            if processed == 0:
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), timeout=poll_interval)
        log.info("worker.stopped")

    async def process_due(self, limit: int | None = None) -> int:
        await self.queue.requeue_expired()
        jobs = await self.queue.reserve(
            limit=limit or self.concurrency * 2, lease_seconds=self.lease_seconds
        )
        if jobs:
            await asyncio.gather(*(self._guarded(job) for job in jobs))
        return len(jobs)

    async def _guarded(self, job: Job) -> None:
        async with self._semaphore:
            try:
                await self.process(job)
            except Exception:
                # Leave the job in-flight: the lease expires and it is retried.
                log.exception("worker.job_crashed", delivery_id=job.delivery_id)

    # -- single job -------------------------------------------------------------------------

    async def process(self, job: Job) -> DeliveryRecord | None:
        delivery = await self.store.get_delivery(job.delivery_id)
        if delivery is None or delivery.is_final:
            await self.queue.ack(job)
            return delivery
        event = await self.store.get_event(job.event_id)
        if event is None:
            await self.queue.ack(job)
            return None

        attempt_no = delivery.attempts_in_cycle + 1
        max_attempts = self._max_attempts(delivery)
        connector = self.destinations.get(delivery.destination)
        started_at = utcnow()
        t0 = time.perf_counter()
        error: DeliveryError | None = None
        outcome = None
        try:
            if connector is None:
                raise DeliveryError("destination is not configured anymore", retryable=False)
            payload = self._render(delivery, event, connector)
            delivery.payload = payload
            timeout = self._timeout(delivery)
            context = DeliveryContext(
                delivery_id=delivery.id, event_id=event.id, attempt=len(delivery.attempts) + 1
            )
            outcome = await asyncio.wait_for(connector.send(payload, context), timeout=timeout)
        except DeliveryError as exc:
            error = exc
        except TimeoutError:
            error = DeliveryError("timed out waiting for the destination", retryable=True)
        except Exception as exc:
            log.exception("worker.connector_crashed", destination=delivery.destination)
            error = DeliveryError(f"unexpected error: {type(exc).__name__}", retryable=True)
        duration = time.perf_counter() - t0

        connector_kind = delivery.connector
        self.metrics.delivery_duration.labels(connector_kind).observe(duration)
        attempt = Attempt(
            number=len(delivery.attempts) + 1,
            started_at=started_at,
            duration_ms=round(duration * 1000, 2),
            ok=error is None,
            status_code=outcome.status_code if outcome else (error.status_code if error else None),
        )
        now = utcnow()
        if error is None:
            delivery.status = DeliveryStatus.DELIVERED
            delivery.result = outcome.result if outcome else None
            delivery.last_error = None
            delivery.next_attempt_at = None
            outcome_label = "delivered"
        else:
            message = redact(error.message, *(connector.secrets() if connector else ()))
            attempt.error = message
            delivery.last_error = message
            if error.retryable and attempt_no < max_attempts:
                delay = self.policy.delay(attempt_no, self.rng)
                if error.retry_after is not None:
                    delay = max(delay, min(error.retry_after, self.policy.max_delay))
                attempt.retry_in_seconds = round(delay, 3)
                delivery.status = DeliveryStatus.RETRYING
                delivery.next_attempt_at = now + timedelta(seconds=delay)
                outcome_label = "retry"
            else:
                delivery.status = DeliveryStatus.DEAD
                delivery.next_attempt_at = None
                outcome_label = "dead"
        delivery.attempts.append(attempt)
        delivery.updated_at = now
        await self.store.save_delivery(delivery)

        if delivery.status is DeliveryStatus.RETRYING:
            assert attempt.retry_in_seconds is not None
            await self.queue.enqueue(job, delay=attempt.retry_in_seconds)
        elif delivery.status is DeliveryStatus.DEAD:
            await self.queue.dead_letter(job, delivery.last_error or "failed")
        await self.queue.ack(job)

        self.metrics.deliveries.labels(delivery.destination, connector_kind, outcome_label).inc()
        log.info(
            "delivery.attempt",
            delivery_id=delivery.id,
            event_id=event.id,
            destination=delivery.destination,
            attempt=attempt.number,
            outcome=outcome_label,
            duration_ms=attempt.duration_ms,
            error=attempt.error,
        )
        return delivery

    def _render(
        self, delivery: DeliveryRecord, event: EventRecord, connector: OutboundConnector
    ) -> object:
        target = self.config.find_target(delivery.route, delivery.destination)
        if target is None:
            raise DeliveryError(
                f"route {delivery.route!r} -> {delivery.destination!r} is not configured anymore",
                retryable=False,
            )
        template = target.template if target.template is not None else connector.default_template
        try:
            return render_payload(template, event.template_context(), connector.escape_fields)
        except TemplateError as exc:
            raise DeliveryError(f"template error: {exc}", retryable=False) from exc

    def _max_attempts(self, delivery: DeliveryRecord) -> int:
        destination = self.config.destinations.get(delivery.destination)
        if destination and destination.max_attempts:
            return destination.max_attempts
        return self.policy.max_attempts

    def _timeout(self, delivery: DeliveryRecord) -> float:
        destination = self.config.destinations.get(delivery.destination)
        if destination and destination.timeout_seconds:
            return destination.timeout_seconds
        return self.default_timeout

    # -- replay -----------------------------------------------------------------------------

    async def replay(self, delivery: DeliveryRecord) -> DeliveryRecord:
        """Put a finished delivery back into the queue with a fresh attempt budget."""
        delivery.status = DeliveryStatus.PENDING
        delivery.attempt_base = len(delivery.attempts)
        delivery.replays += 1
        delivery.last_error = None
        delivery.next_attempt_at = datetime.now(UTC)
        delivery.updated_at = utcnow()
        await self.store.save_delivery(delivery)
        await self.queue.remove_dead(delivery.id)
        await self.queue.enqueue(Job(delivery_id=delivery.id, event_id=delivery.event_id))
        log.info("delivery.replayed", delivery_id=delivery.id, replays=delivery.replays)
        return delivery
