"""End-to-end: HTTP webhook -> ingest -> queue -> worker -> mocked destinations."""

from __future__ import annotations

import asyncio
import contextlib
import http.client
import json
import socket
import struct
import threading
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any
from urllib.parse import urlencode

import fakeredis
import httpx
import pytest
import respx
from prometheus_client.parser import text_string_to_metric_families

from relay.cli import _run_worker
from relay.config import RelayConfig, parse_config
from relay.container import Container
from relay.domain import DeliveryRecord, DeliveryStatus, EventStatus, utcnow
from relay.metrics import MetricsServer, RelayMetrics
from relay.queue import InMemoryQueue, RedisQueue
from relay.queue.base import Job
from relay.security import sign_payload
from relay.settings import Settings
from relay.store.base import EventFilter

from .conftest import FakeClock, LoseNextReply, Workers, no_answer

TG_URL = "https://tg.test/bot123456:SECRET-BOT-TOKEN/sendMessage"
SHEET_URL = "https://sheets.test/v4/spreadsheets/sheet-1/values/Leads%21A%3AC:append"
HOOK_URL = "https://hook.test/in"

FORM = {
    "Name": "Анна <b>",
    "Phone": "8 900 111-22-33",
    "formname": "Заявка",
    "tranid": "t-1",
    "api_key": "tilda-key",
}
FORM_HEADERS = {"content-type": "application/x-www-form-urlencoded"}


def mock_lead_destinations(mock_http: respx.MockRouter) -> tuple[respx.Route, respx.Route]:
    tg = mock_http.post(TG_URL).respond(json={"ok": True, "result": {"message_id": 1}})
    sheet = mock_http.post(SHEET_URL).respond(json={"updates": {"updatedRows": 1}})
    return tg, sheet


async def post_form(api: httpx.AsyncClient, data: dict[str, str] = FORM) -> httpx.Response:
    return await api.post("/webhooks/site", content=urlencode(data), headers=FORM_HEADERS)


async def drain(container: Container, clock: FakeClock, rounds: int = 10) -> None:
    for _ in range(rounds):
        await container.worker.process_due()


class TestWebhookEndpoint:
    async def test_form_is_delivered_everywhere(
        self, api: httpx.AsyncClient, container: Container, mock_http: respx.MockRouter
    ) -> None:
        tg, sheet = mock_lead_destinations(mock_http)
        response = await post_form(api)
        assert response.status_code == 202
        (event_id,) = response.json()["accepted"]
        assert response.headers["x-request-id"]

        await container.worker.process_due()
        event = await container.store.get_event(event_id)
        assert event is not None and event.status is EventStatus.DELIVERED
        text = json.loads(tg.calls.last.request.content)["text"]
        assert text == "<b>Lead</b> Анна &lt;b&gt; +79001112233"  # user input escaped
        row = json.loads(sheet.calls.last.request.content)["values"][0]
        assert row == ["Анна <b>", "+79001112233", event_id]

    async def test_duplicate_is_acknowledged_once(
        self, api: httpx.AsyncClient, container: Container
    ) -> None:
        first = await post_form(api)
        second = await post_form(api)
        assert second.status_code == 200
        assert second.json() == {"accepted": [], "duplicates": first.json()["accepted"]}
        by_header = await api.post(
            "/webhooks/site",
            content=urlencode({**FORM, "tranid": "t-2"}),
            headers={**FORM_HEADERS, "Idempotency-Key": "abc"},
        )
        again = await api.post(
            "/webhooks/site",
            content=urlencode({**FORM, "tranid": "t-3"}),
            headers={**FORM_HEADERS, "Idempotency-Key": "abc"},
        )
        assert by_header.status_code == 202 and again.status_code == 200
        assert (await container.store.stats()).events_total == 2

    async def test_errors(self, api: httpx.AsyncClient, container: Container) -> None:
        assert (await api.post("/webhooks/ghost", content=b"{}")).status_code == 404
        bad_key = await post_form(api, {**FORM, "api_key": "wrong"})
        assert bad_key.status_code == 401
        bad_json = await api.post(
            "/webhooks/partner",
            content=b"{oops",
            headers={
                "content-type": "application/json",
                "x-relay-signature": sign_payload(
                    "partner-secret", b"{oops", int(container.ingest.clock())
                ),
            },
        )
        assert bad_json.status_code == 422
        huge = await api.post("/webhooks/site", content=b"x" * 1_000_001, headers=FORM_HEADERS)
        assert huge.status_code == 413
        ping = await post_form(api, {"test": "test", "api_key": "tilda-key"})
        assert ping.status_code == 200 and ping.text == "ok"
        metrics = (await api.get("/metrics")).text
        assert (
            'relay_events_received_total{result="rejected_signature",source="site"} 1.0' in metrics
        )

    async def test_unmatched_event_is_stored_as_no_route(
        self, api: httpx.AsyncClient, container: Container, clock: FakeClock
    ) -> None:
        body = urlencode({"leads[add][0][id]": "1", "leads[add][0][name]": "x"})
        response = await api.post(
            "/webhooks/crm?token=amo-token", content=body, headers=FORM_HEADERS
        )
        (event_id,) = response.json()["accepted"]
        event = await container.store.get_event(event_id)
        assert event is not None and event.status is EventStatus.NO_ROUTE


class EnqueueFails:
    """A queue whose ``enqueue`` raises after ``ok`` successful calls, like Redis going away."""

    def __init__(self, queue: Any, ok: int = 0) -> None:
        self._queue = queue
        self._ok = ok

    async def enqueue(self, job: Any, *, delay: float = 0.0) -> None:
        if self._ok == 0:
            raise ConnectionError("queue unreachable")
        self._ok -= 1
        await self._queue.enqueue(job, delay=delay)


@pytest.fixture
async def raw_api(container: Container) -> AsyncIterator[httpx.AsyncClient]:
    """Like ``api``, but an unexpected error comes back as a ``500`` response."""
    from relay.api.app import create_app

    app = create_app(container.settings, container=container)
    transport = httpx.ASGITransport(app=app, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://relay.test") as client:
        yield client


class TestIntakeFailure:
    async def test_a_failed_enqueue_lets_the_retry_in(
        self,
        raw_api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        tg, sheet = mock_lead_destinations(mock_http)
        container.ingest.queue = EnqueueFails(container.queue)
        failed = await post_form(raw_api)
        assert failed.status_code == 500
        (first,), total = await container.store.list_events(EventFilter())
        assert total == 1 and first.status is EventStatus.FAILED
        assert {d.status for d in first.deliveries} == {DeliveryStatus.DEAD}
        assert "not queued at intake (ConnectionError)" in (first.deliveries[0].last_error or "")

        container.ingest.queue = container.queue
        retry = await post_form(raw_api)
        assert retry.status_code == 202, retry.text
        (event_id,) = retry.json()["accepted"]
        assert event_id != first.id
        await drain(container, clock)
        assert (tg.call_count, sheet.call_count) == (1, 1)
        event = await container.store.get_event(event_id)
        assert event is not None and event.status is EventStatus.DELIVERED
        again = await container.store.get_event(first.id)
        assert again is not None and again.status is EventStatus.FAILED
        assert all(not d.attempts for d in again.deliveries)

    async def test_a_job_that_got_in_is_not_sent_again_by_the_retry(
        self,
        raw_api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        tg, sheet = mock_lead_destinations(mock_http)
        worker = container.worker

        class SlowToFail(EnqueueFails):
            async def enqueue(self, job: Any, *, delay: float = 0.0) -> None:
                if self._ok == 0:  # a worker takes the first job while this call hangs
                    assert await worker.process_due() == 1
                await super().enqueue(job, delay=delay)

        container.ingest.queue = SlowToFail(container.queue, ok=1)
        assert (await post_form(raw_api)).status_code == 500
        (event,), _ = await container.store.list_events(EventFilter())

        # One destination already has the lead, so the retry may not create a second event.
        container.ingest.queue = container.queue
        retry = await post_form(raw_api)
        later = utcnow() + timedelta(seconds=worker.lease_seconds + 1)
        await worker.sweep(now=later)
        await drain(container, clock)
        assert (tg.call_count, sheet.call_count) == (1, 1)
        assert retry.status_code == 200 and retry.json()["duplicates"] == [event.id]
        delivered = await container.store.get_event(event.id)
        assert delivered is not None and delivered.status is EventStatus.DELIVERED

    async def test_the_sweep_delivers_when_the_key_cannot_be_released(
        self,
        raw_api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        tg, sheet = mock_lead_destinations(mock_http)
        container.ingest.queue = EnqueueFails(container.queue)

        async def release_fails(key: str) -> None:
            raise ConnectionError("idempotency store unreachable")

        monkeypatch.setattr(container.idempotency, "release", release_fails)
        assert (await post_form(raw_api)).status_code == 500
        (event,), _ = await container.store.list_events(EventFilter())
        assert event.status is EventStatus.PROCESSING
        assert {d.status for d in event.deliveries} == {DeliveryStatus.PENDING}

        # The key is still taken, so the retry is a duplicate of the stored event ...
        container.ingest.queue = container.queue
        retry = await post_form(raw_api)
        assert retry.status_code == 200 and retry.json()["duplicates"] == [event.id]
        # ... and the sweep queues its deliveries once they are a lease overdue.
        worker = container.worker
        assert await worker.sweep() == 0
        later = utcnow() + timedelta(seconds=worker.lease_seconds + 1)
        assert await worker.sweep(now=later) == 2
        await drain(container, clock)
        assert (tg.call_count, sheet.call_count) == (1, 1)
        delivered = await container.store.get_event(event.id)
        assert delivered is not None and delivered.status is EventStatus.DELIVERED

    async def test_the_key_stays_taken_when_the_event_cannot_be_marked(
        self,
        raw_api: httpx.AsyncClient,
        container: Container,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        container.ingest.queue = EnqueueFails(container.queue)

        async def store_fails(delivery: Any) -> None:
            raise OSError("disk full")

        monkeypatch.setattr(container.store, "save_delivery", store_fails)
        assert (await post_form(raw_api)).status_code == 500
        monkeypatch.undo()
        (event,), _ = await container.store.list_events(EventFilter())
        assert {d.status for d in event.deliveries} == {DeliveryStatus.PENDING}
        # Releasing the key now would get the lead delivered twice: once as this event,
        # through the sweep, and once as the retry.
        container.ingest.queue = container.queue
        retry = await post_form(raw_api)
        assert retry.status_code == 200 and retry.json()["duplicates"] == [event.id]


@pytest.fixture(params=["memory", "redis"])
async def worker_queue(request: pytest.FixtureRequest, clock: FakeClock) -> AsyncIterator[Any]:
    if request.param == "memory":
        yield InMemoryQueue(clock=clock)
    else:
        queue = RedisQueue(fakeredis.FakeAsyncRedis(), prefix="t", clock=clock)
        yield queue
        await queue.close()


@pytest.fixture(params=["memory", "redis"])
def two_workers(
    request: pytest.FixtureRequest, container: Container, workers: Workers
) -> tuple[Any, Any]:
    """Points intake and the worker at one queue. Returns it and another worker's handle on it.

    The Redis case uses separate connections to fakeredis, or to a real Redis when
    ``RELAY_TEST_REDIS_URL`` is set.
    """
    if request.param == "memory":
        queue = other = container.queue
    else:
        _, (queue, other) = workers(2)
    container.ingest.queue = container.worker.queue = queue
    return queue, other


class TestSweep:
    async def test_deliveries_that_lost_their_job_are_queued_again(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
        worker_queue: Any,
    ) -> None:
        tg, sheet = mock_lead_destinations(mock_http)
        # Intake queued the jobs in a memory queue that went away with the old process.
        container.ingest.queue = InMemoryQueue(clock=clock)
        worker = container.worker
        worker.queue = worker_queue
        (event_id,) = (await post_form(api)).json()["accepted"]

        assert await worker.sweep() == 0, "fresh deliveries may still be on their way in"
        later = utcnow() + timedelta(seconds=worker.lease_seconds + 1)
        assert await worker.sweep(now=later) == 2
        assert await worker.sweep(now=later) == 0, "a queued job is not added twice"
        (job,) = await worker_queue.reserve(limit=1, lease_seconds=worker.lease_seconds)
        assert await worker.sweep(now=later) == 0, "nor is a job a worker holds"
        await worker.process(job)
        await drain(container, clock)
        assert (tg.call_count, sheet.call_count) == (1, 1)
        event = await container.store.get_event(event_id)
        assert event is not None and event.status is EventStatus.DELIVERED
        assert await worker.sweep(now=later) == 0

    async def test_the_worker_sweeps_when_it_starts(
        self, api: httpx.AsyncClient, container: Container, mock_http: respx.MockRouter
    ) -> None:
        tg, sheet = mock_lead_destinations(mock_http)
        container.ingest.queue = InMemoryQueue()
        (event_id,) = (await post_form(api)).json()["accepted"]
        event = await container.store.get_event(event_id)
        assert event is not None
        for delivery in event.deliveries:  # stored before the restart
            delivery.updated_at -= timedelta(hours=1)
            await container.store.save_delivery(delivery)

        stop = asyncio.Event()
        task = asyncio.create_task(container.worker.run(stop, poll_interval=0.01))
        for _ in range(200):
            if tg.call_count and sheet.call_count:
                break
            await asyncio.sleep(0.01)
        stop.set()
        await task
        assert (tg.call_count, sheet.call_count) == (1, 1)


class TestRetries:
    async def signed_partner_event(
        self, api: httpx.AsyncClient, clock: FakeClock, payload: dict[str, Any]
    ) -> str:
        body = json.dumps(payload).encode()
        response = await api.post(
            "/webhooks/partner",
            content=body,
            headers={
                "content-type": "application/json",
                "x-relay-signature": sign_payload("partner-secret", body, int(clock())),
            },
        )
        assert response.status_code == 202, response.text
        return str(response.json()["accepted"][0])

    async def test_backoff_then_success(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        route = mock_http.post(HOOK_URL).mock(
            side_effect=[httpx.Response(503), httpx.Response(200)]
        )
        event_id = await self.signed_partner_event(api, clock, {"type": "ping", "id": "1"})
        await container.worker.process_due()
        event = await container.store.get_event(event_id)
        assert event is not None
        (delivery,) = event.deliveries
        assert delivery.status is DeliveryStatus.RETRYING
        assert delivery.attempts[0].retry_in_seconds == 2.0  # base delay, jitter disabled
        assert event.status is EventStatus.PROCESSING

        clock.advance(1)
        assert await container.worker.process_due() == 0  # not due yet
        clock.advance(1)
        assert await container.worker.process_due() == 1
        delivered = await container.store.get_delivery(delivery.id)
        assert delivered is not None and delivered.status is DeliveryStatus.DELIVERED
        assert route.call_count == 2
        second = route.calls.last.request
        assert second.headers["x-relay-attempt"] == "2"
        assert second.headers["idempotency-key"] == delivery.id

    async def test_a_worker_that_lost_its_lease_leaves_the_record_alone(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        route = mock_http.post(HOOK_URL).mock(
            side_effect=[httpx.Response(200), httpx.Response(503)]
        )
        event_id = await self.signed_partner_event(api, clock, {"type": "x", "id": "3"})
        worker, queue = container.worker, container.queue
        hook = worker.destinations["hook"]
        send = hook.send
        calls = 0

        async def slow_send(payload: Any, context: Any) -> Any:
            nonlocal calls
            calls += 1
            if calls == 1:
                # Worker x is still waiting on the destination. Its lease runs out, worker y
                # takes the job over and delivers it; then x's request fails with 503.
                clock.advance(worker.lease_seconds + 1)
                assert await queue.requeue_expired() == 1
                (y_job,) = await queue.reserve(limit=1, lease_seconds=worker.lease_seconds)
                await worker.process(y_job)
            return await send(payload, context)

        hook.send = slow_send  # type: ignore[method-assign]
        assert await worker.process_due() == 1

        event = await container.store.get_event(event_id)
        assert event is not None
        (delivery,) = event.deliveries
        assert delivery.status is DeliveryStatus.DELIVERED, "x must not write its retry over y"
        assert [a.ok for a in delivery.attempts] == [True]
        depth = await queue.depth()
        assert (depth.ready, depth.scheduled, depth.in_flight, depth.dead) == (0, 0, 0, 0)
        clock.advance(60)
        assert await worker.process_due() == 0
        assert route.call_count == 2
        lost = worker.metrics.registry.get_sample_value(
            "relay_delivery_attempts_total",
            {"destination": "hook", "connector": "webhook", "outcome": "lease_lost"},
        )
        assert lost == 1, "x's request went out, so it is counted"

    async def test_a_queue_error_before_saving_does_not_cause_a_second_send(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        route = mock_http.post(HOOK_URL).respond(200)
        event_id = await self.signed_partner_event(api, clock, {"type": "x", "id": "6"})
        worker, queue = container.worker, container.queue
        extend, ack = queue.extend, queue.ack
        extends = 0

        async def flaky_extend(job: Any, *, lease_seconds: float) -> Any:
            nonlocal extends
            extends += 1
            if extends == 2:  # the renewal right before saving
                raise ConnectionError("redis went away")
            return await extend(job, lease_seconds=lease_seconds)

        async def failing_ack(job: Any) -> bool:
            raise ConnectionError("redis went away")

        queue.extend = flaky_extend  # type: ignore[method-assign]
        queue.ack = failing_ack  # type: ignore[method-assign]
        assert await worker.process_due() == 1  # the job crashes after the send and the save

        event = await container.store.get_event(event_id)
        assert event is not None
        (delivery,) = event.deliveries
        assert delivery.status is DeliveryStatus.DELIVERED
        queue.ack = ack  # type: ignore[method-assign]
        clock.advance(worker.lease_seconds + 1)
        assert await worker.process_due() == 1  # the lease ran out, the job runs again
        assert route.call_count == 1, "the re-run finds the attempt and does not send"
        assert (await queue.depth()).in_flight == 0

    async def test_a_job_that_waited_for_a_slot_gets_a_fresh_lease(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        mock_http.post(HOOK_URL).respond(200)
        await self.signed_partner_event(api, clock, {"type": "x", "id": "4"})
        worker, queue = container.worker, container.queue
        (job,) = await queue.reserve(limit=1, lease_seconds=worker.lease_seconds)
        clock.advance(worker.lease_seconds - 1)  # almost the whole lease spent waiting
        hook = worker.destinations["hook"]
        send = hook.send

        async def slow_send(payload: Any, context: Any) -> Any:
            clock.advance(10)  # past the deadline from reserve
            assert await queue.requeue_expired() == 0, "process() renews the lease first"
            return await send(payload, context)

        hook.send = slow_send  # type: ignore[method-assign]
        delivery = await worker.process(job)
        assert delivery is not None and delivery.status is DeliveryStatus.DELIVERED
        assert (await queue.depth()).in_flight == 0

    async def test_a_job_taken_over_while_waiting_is_not_sent(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        route = mock_http.post(HOOK_URL).respond(200)
        await self.signed_partner_event(api, clock, {"type": "x", "id": "5"})
        worker, queue = container.worker, container.queue
        (x_job,) = await queue.reserve(limit=1, lease_seconds=worker.lease_seconds)
        clock.advance(worker.lease_seconds + 1)
        assert await queue.requeue_expired() == 1
        (y_job,) = await queue.reserve(limit=1, lease_seconds=worker.lease_seconds)

        assert await worker.process(x_job) is None
        assert route.call_count == 0
        delivered = await worker.process(y_job)
        assert delivered is not None and delivered.status is DeliveryStatus.DELIVERED
        assert route.call_count == 1

    # While the connector runs, a heartbeat renews the lease every ``heartbeat_seconds``. The
    # tests shorten it and move the injected clock from inside the request.

    async def test_the_heartbeat_keeps_the_lease_through_a_slow_request(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
        two_workers: tuple[Any, Any],
    ) -> None:
        route = mock_http.post(HOOK_URL).respond(200)
        queue, other = two_workers
        event_id = await self.signed_partner_event(api, clock, {"type": "x", "id": "7"})
        worker = container.worker
        worker.heartbeat_seconds = 0.001
        extend = queue.extend
        renewed = asyncio.Event()

        async def watched_extend(job: Any, *, lease_seconds: float) -> Any:
            held = await extend(job, lease_seconds=lease_seconds)
            renewed.set()
            return held

        hook = worker.destinations["hook"]
        send = hook.send
        taken: list[Job] = []

        async def slow_send(payload: Any, context: Any) -> Any:
            for _ in range(4):  # the request takes two leases
                renewed.clear()
                clock.advance(worker.lease_seconds / 2)
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(renewed.wait(), timeout=1)  # the next heartbeat
                await other.requeue_expired()
                taken.extend(await other.reserve(limit=1, lease_seconds=worker.lease_seconds))
            return await send(payload, context)

        queue.extend = watched_extend
        hook.send = slow_send  # type: ignore[method-assign]
        assert await worker.process_due() == 1

        assert taken == [], "another worker took the job over mid-request"
        assert route.call_count == 1
        event = await container.store.get_event(event_id)
        assert event is not None
        (delivery,) = event.deliveries
        # The write is fenced with the newest token the heartbeat got.
        assert delivery.status is DeliveryStatus.DELIVERED
        assert [a.ok for a in delivery.attempts] == [True]
        depth = await queue.depth()
        assert (depth.ready, depth.scheduled, depth.in_flight, depth.dead) == (0, 0, 0, 0)

    async def test_a_heartbeat_that_finds_the_lease_lost_cancels_the_request(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
        two_workers: tuple[Any, Any],
    ) -> None:
        route = mock_http.post(HOOK_URL).respond(200)
        queue, other = two_workers
        event_id = await self.signed_partner_event(api, clock, {"type": "x", "id": "8"})
        worker = container.worker
        worker.heartbeat_seconds = 0.001
        extend = queue.extend
        extends = 0
        taken_over = asyncio.Event()

        async def held_back_extend(job: Any, *, lease_seconds: float) -> Any:
            nonlocal extends
            extends += 1
            if extends > 1:  # renewals after the one at the start wait for y
                await taken_over.wait()
            return await extend(job, lease_seconds=lease_seconds)

        hook = worker.destinations["hook"]
        send = hook.send
        y_jobs: list[Job] = []
        cancelled: list[int] = []

        async def hanging_send(payload: Any, context: Any) -> Any:
            # x's lease runs out while the destination hangs, and worker y takes the job over.
            clock.advance(worker.lease_seconds + 1)
            await other.requeue_expired()
            y_jobs.extend(await other.reserve(limit=1, lease_seconds=worker.lease_seconds))
            taken_over.set()
            try:
                await asyncio.sleep(1)
            except asyncio.CancelledError:
                cancelled.append(context.attempt)
                raise
            return await send(payload, context)

        queue.extend = held_back_extend
        hook.send = hanging_send  # type: ignore[method-assign]
        assert await worker.process_due() == 1

        assert cancelled == [1], "the heartbeat must cancel x's request"
        assert route.call_count == 0
        stored = await container.store.get_event(event_id)
        assert stored is not None
        (delivery,) = stored.deliveries
        assert delivery.status is DeliveryStatus.PENDING and delivery.attempts == []
        lost = worker.metrics.registry.get_sample_value(
            "relay_delivery_attempts_total",
            {"destination": "hook", "connector": "webhook", "outcome": "lease_lost"},
        )
        assert lost == 1

        hook.send = send  # type: ignore[method-assign]
        (y_job,) = y_jobs
        delivered = await worker.process(y_job)
        assert delivered is not None and delivered.status is DeliveryStatus.DELIVERED
        assert [a.ok for a in delivered.attempts] == [True]
        assert route.call_count == 1
        assert (await queue.depth()).in_flight == 0

    async def test_a_queue_error_during_the_heartbeat_does_not_stop_the_request(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
        two_workers: tuple[Any, Any],
    ) -> None:
        route = mock_http.post(HOOK_URL).respond(200)
        queue, other = two_workers
        event_id = await self.signed_partner_event(api, clock, {"type": "x", "id": "9"})
        worker = container.worker
        worker.heartbeat_seconds = 0.001
        extend = queue.extend
        extends = 0
        failed, renewed = asyncio.Event(), asyncio.Event()

        async def flaky_extend(job: Any, *, lease_seconds: float) -> Any:
            nonlocal extends
            extends += 1
            if extends == 2:  # the first heartbeat
                failed.set()
                raise ConnectionError("redis went away")
            held = await extend(job, lease_seconds=lease_seconds)
            if extends > 2:
                renewed.set()
            return held

        hook = worker.destinations["hook"]
        send = hook.send
        taken: list[Job] = []

        async def slow_send(payload: Any, context: Any) -> Any:
            clock.advance(worker.lease_seconds / 2)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(failed.wait(), timeout=1)
                await asyncio.wait_for(renewed.wait(), timeout=1)  # the next heartbeat
            clock.advance(worker.lease_seconds / 2 + 1)  # past the lease from the start
            await other.requeue_expired()
            taken.extend(await other.reserve(limit=1, lease_seconds=worker.lease_seconds))
            return await send(payload, context)

        queue.extend = flaky_extend
        hook.send = slow_send  # type: ignore[method-assign]
        assert await worker.process_due() == 1

        assert taken == [], "another worker took the job over mid-request"
        assert failed.is_set()
        assert route.call_count == 1
        event = await container.store.get_event(event_id)
        assert event is not None
        (delivery,) = event.deliveries
        assert delivery.status is DeliveryStatus.DELIVERED
        assert [a.ok for a in delivery.attempts] == [True]
        assert (await queue.depth()).in_flight == 0

    async def test_a_heartbeat_whose_reply_was_lost_does_not_cancel_the_request(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
        workers: Workers,
    ) -> None:
        route = mock_http.post(HOOK_URL).respond(200)
        clients, (queue, other) = workers(2)
        container.ingest.queue = container.worker.queue = queue
        event_id = await self.signed_partner_event(api, clock, {"type": "x", "id": "10"})
        worker = container.worker
        worker.heartbeat_seconds = 0.001
        extend = queue.extend
        renewed = asyncio.Event()

        async def watched_extend(job: Any, *, lease_seconds: float) -> Any:
            held = await extend(job, lease_seconds=lease_seconds)
            if held is not None:
                renewed.set()
            return held

        hook = worker.destinations["hook"]
        send = hook.send
        taken: list[Job] = []

        async def slow_send(payload: Any, context: Any) -> Any:
            clock.advance(worker.lease_seconds / 2)
            # Redis applies the next heartbeat, but its reply is lost on the way back.
            lose = LoseNextReply(clients[0])
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(lose.lost.wait(), timeout=1)
                renewed.clear()
                await asyncio.wait_for(renewed.wait(), timeout=1)  # the heartbeat after it
            clock.advance(worker.lease_seconds / 2 + 1)  # past the lease from the start
            await other.requeue_expired()
            taken.extend(await other.reserve(limit=1, lease_seconds=worker.lease_seconds))
            return await send(payload, context)

        queue.extend = watched_extend  # type: ignore[method-assign]
        hook.send = slow_send  # type: ignore[method-assign]
        assert await worker.process_due() == 1

        assert route.call_count == 1, "the worker cancelled its own request"
        assert taken == [], "another worker took the job over mid-request"
        event = await container.store.get_event(event_id)
        assert event is not None
        (delivery,) = event.deliveries
        assert delivery.status is DeliveryStatus.DELIVERED
        assert [a.ok for a in delivery.attempts] == [True]
        assert (await queue.depth()).in_flight == 0

    async def test_dead_letter_and_replay(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        route = mock_http.post(HOOK_URL).respond(500)
        event_id = await self.signed_partner_event(api, clock, {"type": "x", "id": "2"})
        for _ in range(3):  # destination max_attempts: 3 -> delays 2s, 4s
            await container.worker.process_due()
            clock.advance(10)
        event = await container.store.get_event(event_id)
        assert event is not None and event.status is EventStatus.FAILED
        (delivery,) = event.deliveries
        assert delivery.status is DeliveryStatus.DEAD
        assert [a.retry_in_seconds for a in delivery.attempts] == [2.0, 4.0, None]
        assert route.call_count == 3

        dead = (await api.get("/admin/api/dead-letters")).json()
        assert [d["delivery"]["id"] for d in dead] == [delivery.id]
        assert dead[0]["event"]["id"] == event_id
        assert (await container.queue.depth()).dead == 1

        route.respond(200)
        replay = await api.post(f"/admin/api/events/{event_id}/replay")
        assert replay.json() == {"replayed": [delivery.id], "skipped": []}
        assert (await container.queue.depth()).dead == 0
        await container.worker.process_due()
        replayed = await container.store.get_delivery(delivery.id)
        assert replayed is not None and replayed.status is DeliveryStatus.DELIVERED
        assert replayed.replays == 1 and len(replayed.attempts) == 4
        assert (await container.store.get_event(event_id)).status is EventStatus.DELIVERED  # type: ignore[union-attr]

    async def test_retry_after_hint_and_secret_redaction(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        mock_http.post(TG_URL).respond(
            429, json={"ok": False, "description": "slow down", "parameters": {"retry_after": 40}}
        )
        mock_http.post(SHEET_URL).mock(side_effect=httpx.ConnectError("sheets-token leaked?"))
        response = await post_form(api)
        await container.worker.process_due()
        event = await container.store.get_event(response.json()["accepted"][0])
        assert event is not None
        tg, sheet = event.deliveries
        assert tg.attempts[0].retry_in_seconds == 40
        assert sheet.last_error is not None and "sheets-token" not in sheet.last_error

    async def test_template_error_is_permanent(
        self,
        container: Container,
        api: httpx.AsyncClient,
        mock_http: respx.MockRouter,
    ) -> None:
        container.config.routes[0].deliver[0].template = {"text": "{{ x | nope }}"}
        mock_http.post(SHEET_URL).respond(json={})
        response = await post_form(api)
        await container.worker.process_due()
        event = await container.store.get_event(response.json()["accepted"][0])
        assert event is not None and event.status is EventStatus.PARTIAL
        assert "unknown filter" in (event.deliveries[0].last_error or "")


AMO_LEAD_URL = "https://demo.amocrm.ru/api/v4/leads/complex"
AMO_NOTE_URL = "https://demo.amocrm.ru/api/v4/leads/notes"
AMO_CONFIG = """
version: 1
sources:
  site:
    connector: tilda
    secret: tilda-key
destinations:
  amo:
    connector: amocrm_lead
    timeout_seconds: 1
    options:
      subdomain: demo
      access_token: amo-secret
routes:
  - name: leads
    match: {source: site}
    deliver:
      - to: amo
"""


class TestAmoCrmNote:
    """Once the lead exists, nothing that happens to the note may create it a second time."""

    @pytest.fixture
    def config(self) -> RelayConfig:
        return parse_config(AMO_CONFIG, env={})

    async def deliver_lead(
        self, api: httpx.AsyncClient, container: Container, clock: FakeClock
    ) -> DeliveryRecord:
        form = {**FORM, "Comment": "Перезвоните после обеда"}
        (event_id,) = (await post_form(api, form)).json()["accepted"]
        assert await container.worker.process_due() == 1
        clock.advance(3600)  # past any retry the attempt could have scheduled
        await drain(container, clock)
        event = await container.store.get_event(event_id)
        assert event is not None
        (delivery,) = event.deliveries
        return delivery

    async def test_a_network_error_on_the_note_is_not_retried(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        lead = mock_http.post(AMO_LEAD_URL).respond(json=[{"id": 100, "contact_id": 200}])
        note = mock_http.post(AMO_NOTE_URL).mock(side_effect=httpx.ConnectError("reset"))
        delivery = await self.deliver_lead(api, container, clock)

        assert lead.call_count == 1, "the retry created a second lead"
        assert note.call_count == 1
        assert delivery.status is DeliveryStatus.DELIVERED
        assert [a.ok for a in delivery.attempts] == [True]
        assert delivery.result == {
            "lead_id": 100,
            "contact_id": 200,
            "note_error": "network error (ConnectError: reset)",
        }
        depth = await container.queue.depth()
        assert (depth.ready, depth.scheduled, depth.in_flight, depth.dead) == (0, 0, 0, 0)

    async def test_a_note_that_never_answers_does_not_run_the_attempt_out(
        self,
        api: httpx.AsyncClient,
        container: Container,
        clock: FakeClock,
        mock_http: respx.MockRouter,
    ) -> None:
        lead = mock_http.post(AMO_LEAD_URL).respond(json=[{"id": 100, "contact_id": 200}])
        mock_http.post(AMO_NOTE_URL).mock(side_effect=no_answer)
        delivery = await self.deliver_lead(api, container, clock)

        assert lead.call_count == 1, "the retry created a second lead"
        assert delivery.status is DeliveryStatus.DELIVERED, delivery.last_error
        assert delivery.result is not None and delivery.result["lead_id"] == 100
        assert delivery.result["note_error"].startswith("timeout")


class TestAdminApi:
    async def test_events_stats_connectors(
        self, api: httpx.AsyncClient, container: Container, mock_http: respx.MockRouter
    ) -> None:
        mock_lead_destinations(mock_http)
        event_id = (await post_form(api)).json()["accepted"][0]
        await container.worker.process_due()

        page = (await api.get("/admin/api/events", params={"q": "анна", "limit": 5})).json()
        assert page["total"] == 1 and page["items"][0]["id"] == event_id
        assert page["items"][0]["deliveries"][0]["attempts"] == 1
        empty = (await api.get("/admin/api/events", params={"status": "failed"})).json()
        assert empty["total"] == 0
        detail = (await api.get(f"/admin/api/events/{event_id}")).json()
        assert detail["contact"]["phone"] == "8 900 111-22-33"
        assert (await api.get("/admin/api/events/nope")).status_code == 404

        stats = (await api.get("/admin/api/stats")).json()
        assert stats["events_by_status"] == {"delivered": 1}
        assert stats["queue"] == {"ready": 0, "scheduled": 0, "in_flight": 0, "dead": 0}

        connectors = (await api.get("/admin/api/connectors")).json()
        assert "SECRET-BOT-TOKEN" not in json.dumps(connectors)
        assert {c["kind"] for c in connectors["available"]["outbound"]} >= {
            "telegram",
            "webhook",
            "google_sheets",
            "amocrm_lead",
            "smtp",
        }
        assert connectors["retry_schedule_seconds"] == [2.0, 4.0, 8.0]

    async def test_replay_single_delivery(
        self, api: httpx.AsyncClient, container: Container, mock_http: respx.MockRouter
    ) -> None:
        mock_lead_destinations(mock_http)
        event_id = (await post_form(api)).json()["accepted"][0]
        event = await container.store.get_event(event_id)
        assert event is not None
        delivery_id = event.deliveries[0].id
        conflict = await api.post(f"/admin/api/deliveries/{delivery_id}/replay")
        assert conflict.status_code == 409
        await container.worker.process_due()
        skipped = await api.post(f"/admin/api/events/{event_id}/replay")
        assert len(skipped.json()["skipped"]) == 2
        forced = await api.post(f"/admin/api/deliveries/{delivery_id}/replay")
        assert forced.json()["status"] == "pending"
        assert (await api.post("/admin/api/deliveries/nope/replay")).status_code == 404

    async def test_admin_token(self, container: Container, api: httpx.AsyncClient) -> None:
        from pydantic import SecretStr

        container.settings.admin_token = SecretStr("adm1n")
        assert (await api.get("/admin/api/stats")).status_code == 401
        wrong = await api.get("/admin/api/stats", headers={"Authorization": "Bearer nope"})
        assert wrong.status_code == 401
        ok = await api.get("/admin/api/stats", headers={"Authorization": "Bearer adm1n"})
        assert ok.status_code == 200
        container.settings.admin_token = None
        container.settings.env = "prod"
        assert (await api.get("/admin/api/stats")).status_code == 503


class TestOps:
    async def test_health_ready_metrics(self, api: httpx.AsyncClient) -> None:
        assert (await api.get("/healthz")).json()["status"] == "ok"
        ready = await api.get("/readyz")
        assert ready.json() == {"status": "ready", "checks": {"store": True, "queue": True}}
        metrics = await api.get("/metrics")
        assert "relay_queue_jobs" in metrics.text and "relay_build_info" in metrics.text


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port: int = sock.getsockname()[1]
        return port


async def http_get(port: int, path: str) -> tuple[int, str]:
    """A real HTTP request, which respx does not intercept."""

    def request() -> tuple[int, str]:
        conn = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
        try:
            conn.request("GET", path)
            response = conn.getresponse()
            return response.status, response.read().decode()
        finally:
            conn.close()

    return await asyncio.to_thread(request)


def sample(text: str, name: str, **labels: str) -> float | None:
    for family in text_string_to_metric_families(text):
        for s in family.samples:
            if s.name == name and s.labels == labels:
                return float(s.value)
    return None


class TestWorkerMetrics:
    """``relay worker`` has no API, so its delivery series need a port of their own."""

    async def test_the_worker_serves_its_delivery_metrics(
        self,
        settings: Settings,
        container: Container,
        api: httpx.AsyncClient,
        mock_http: respx.MockRouter,
    ) -> None:
        mock_lead_destinations(mock_http)
        port = free_port()
        worker_settings = settings.model_copy(
            update={"worker_metrics_port": port, "worker_poll_interval": 0.01}
        )
        stop = asyncio.Event()
        task = asyncio.create_task(_run_worker(worker_settings, container, stop))
        try:
            assert (await post_form(api)).status_code == 202
            attempts = "relay_delivery_attempts_total"
            tg = {"destination": "tg", "connector": "telegram", "outcome": "delivered"}
            sheet = {"destination": "sheet", "connector": "google_sheets", "outcome": "delivered"}
            status, text = 0, ""
            for _ in range(200):
                with contextlib.suppress(ConnectionRefusedError):  # not listening yet
                    status, text = await http_get(port, "/metrics")
                    if sample(text, attempts, **tg) and sample(text, attempts, **sheet):
                        break
                await asyncio.sleep(0.01)
            assert status == 200
            assert sample(text, attempts, **tg) == 1
            assert sample(text, attempts, **sheet) == 1
            count = "relay_delivery_duration_seconds_count"
            assert sample(text, count, connector="telegram") == 1
            assert "relay-metrics" in [thread.name for thread in threading.enumerate()]

            status, body = await http_get(port, "/healthz")
            assert status == 200 and json.loads(body)["status"] == "ok"
            assert (await http_get(port, "/"))[0] == 404
        finally:
            stop.set()
            await task
        with pytest.raises(ConnectionRefusedError):
            await http_get(port, "/metrics")

    async def test_no_server_without_the_setting(
        self, settings: Settings, container: Container
    ) -> None:
        assert settings.worker_metrics_port is None
        stop = asyncio.Event()
        worker_settings = settings.model_copy(update={"worker_poll_interval": 0.01})
        task = asyncio.create_task(_run_worker(worker_settings, container, stop))
        await asyncio.sleep(0.05)
        threads = [thread.name for thread in threading.enumerate()]
        stop.set()
        await task
        assert "relay-metrics" not in threads


def join_request_threads() -> None:
    for thread in threading.enumerate():
        if thread.name.endswith("(process_request_thread)"):
            thread.join(timeout=5)


class TestMetricsServer:
    """stderr carries the JSON log, one object per line, so the server must not print there."""

    async def test_a_client_that_hangs_up_leaves_stderr_alone(
        self, capsys: pytest.CaptureFixture[str]
    ) -> None:
        port = free_port()
        server = MetricsServer(RelayMetrics(), "127.0.0.1", port)
        try:
            with socket.create_connection(("127.0.0.1", port)) as sock:
                sock.sendall(b"GET /metr")
                # close with a reset, like a scraper that gives up mid-request
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            # accepted after the reset connection, so that one has its thread by now
            assert (await http_get(port, "/healthz"))[0] == 200
        finally:
            server.close()
        join_request_threads()
        assert capsys.readouterr().err == ""


@pytest.mark.parametrize("path", ["/openapi.json", "/docs"])
async def test_openapi_available(api: httpx.AsyncClient, path: str) -> None:
    assert (await api.get(path)).status_code == 200


async def test_prod_refuses_dev_placeholder_secrets(settings: Settings) -> None:
    from relay.config import load_config
    from relay.container import build_container
    from relay.errors import ConfigError

    from .conftest import REPO_ROOT

    repo_config = load_config(REPO_ROOT / "config" / "relay.yaml", env={})
    prod = settings.model_copy(update={"env": "prod"})
    with pytest.raises(ConfigError, match="placeholder secrets"):
        await build_container(prod, repo_config)
