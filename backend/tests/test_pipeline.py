"""End-to-end: HTTP webhook -> ingest -> queue -> worker -> mocked destinations."""

from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlencode

import httpx
import pytest
import respx

from relay.container import Container
from relay.domain import DeliveryStatus, EventStatus
from relay.security import sign_payload
from relay.settings import Settings

from .conftest import FakeClock

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
