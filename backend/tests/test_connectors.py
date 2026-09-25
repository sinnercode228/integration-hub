"""Inbound parsing/verification and outbound delivery against mocked HTTP (respx)."""

from __future__ import annotations

import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any
from urllib.parse import parse_qs, urlencode

import aiosmtplib
import httpx
import jwt
import pytest
import respx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from relay.connectors.base import DeliveryContext, InboundRequest
from relay.connectors.inbound import (
    AmoCrmInbound,
    Bitrix24Inbound,
    GenericJsonInbound,
    TildaInbound,
)
from relay.connectors.outbound import (
    AmoCrmLeadConnector,
    GoogleSheetsConnector,
    SmtpEmailConnector,
    TelegramConnector,
    WebhookConnector,
)
from relay.connectors.outbound.google_sheets import ServiceAccountTokenProvider
from relay.errors import ConfigError, DeliveryError, PayloadError, SignatureError
from relay.security import sign_payload, verify_signature

NOW = 1_760_000_000
CTX = DeliveryContext(delivery_id="dlv_1", event_id="evt_1", attempt=1)


def req(
    body: bytes | str | Mapping[str, Any],
    *,
    content_type: str = "application/x-www-form-urlencoded",
    headers: Mapping[str, str] | None = None,
    query: Mapping[str, str] | None = None,
) -> InboundRequest:
    if isinstance(body, Mapping):
        body = urlencode(body)
    raw = body.encode() if isinstance(body, str) else body
    return InboundRequest(
        body=raw,
        headers={k.lower(): v for k, v in (headers or {}).items()},
        query=dict(query or {}),
        content_type=content_type,
        received_at=datetime.now(UTC),
    )


@pytest.fixture
async def http() -> Any:
    async with httpx.AsyncClient() as client:
        yield client


# -- inbound ---------------------------------------------------------------------------------


class TestTilda:
    def make(self, http: httpx.AsyncClient) -> TildaInbound:
        return TildaInbound("site", secret="key", options={}, http=http)

    async def test_form(self, http: httpx.AsyncClient) -> None:
        conn = self.make(http)
        request = req(
            {
                "Name": "Анна",
                "Phone": "+7 (900) 111-22-33",
                "Email": "anna@example.com",
                "Comment": "Перезвоните",
                "formid": "form1",
                "formname": "Заявка",
                "tranid": "123:456",
                "api_key": "key",
                "COOKIES": "utm_source=yandex; _ga=1; utm_campaign=spring",
            }
        )
        conn.verify(request, now=NOW)
        result = await conn.parse(request)
        (event,) = result.events
        assert event.type == "form.submitted"
        assert event.external_id == "123:456"
        assert event.contact.name == "Анна" and event.contact.email == "anna@example.com"
        assert event.fields["comment"] == "Перезвоните"
        assert event.fields["utm"] == {"utm_source": "yandex", "utm_campaign": "spring"}
        assert "api_key" not in event.raw  # the key is never stored

    async def test_order(self, http: httpx.AsyncClient) -> None:
        payment = {"orderid": "9001", "amount": "2500", "products": [{"name": "Mug"}]}
        request = req(
            json.dumps({"name": "Ivan", "api_key": "key", "payment": payment}),
            content_type="application/json",
        )
        conn = self.make(http)
        conn.verify(request, now=NOW)
        (event,) = (await conn.parse(request)).events
        assert event.type == "order.created"
        assert event.fields["order"]["amount"] == "2500"
        assert event.external_id == "9001"

    async def test_ping_and_auth(self, http: httpx.AsyncClient) -> None:
        conn = self.make(http)
        assert (await conn.parse(req({"test": "test"}))).ping
        conn.verify(req({}, headers={"X-Api-Key": "key"}), now=NOW)
        with pytest.raises(SignatureError):
            conn.verify(req({"api_key": "wrong"}), now=NOW)
        with pytest.raises(SignatureError):
            conn.verify(req({"name": "x"}), now=NOW)


class TestAmoCrm:
    async def test_multiple_entities_become_events(self, http: httpx.AsyncClient) -> None:
        conn = AmoCrmInbound("crm", secret="tok", options={}, http=http)
        body = urlencode(
            {
                "leads[status][0][id]": "11",
                "leads[status][0][status_id]": "142",
                "leads[status][0][name]": "Deal",
                "leads[status][0][last_modified]": "1700000000",
                "contacts[add][0][id]": "5",
                "contacts[add][0][name]": "Olga",
                "contacts[add][0][custom_fields][0][code]": "PHONE",
                "contacts[add][0][custom_fields][0][name]": "Телефон",
                "contacts[add][0][custom_fields][0][values][0][value]": "+79000000000",
                "account[subdomain]": "demo",
            }
        )
        request = req(body, query={"token": "tok"})
        conn.verify(request, now=NOW)
        events = (await conn.parse(request)).events
        assert [e.type for e in events] == ["lead.status_changed", "contact.created"]
        assert events[0].fields["status_id"] == "142"
        assert events[0].external_id == "lead:11:status:1700000000"
        assert events[1].contact.phone == "+79000000000"
        assert events[1].fields["custom"] == {"Телефон": "+79000000000"}

    async def test_rejects(self, http: httpx.AsyncClient) -> None:
        conn = AmoCrmInbound("crm", secret="tok", options={}, http=http)
        with pytest.raises(SignatureError):
            conn.verify(req("", query={"token": "nope"}), now=NOW)
        with pytest.raises(PayloadError):
            await conn.parse(req("foo=bar"))


class TestBitrix24:
    body = urlencode(
        {
            "event": "ONCRMLEADADD",
            "data[FIELDS][ID]": "77",
            "ts": "1700000000",
            "auth[application_token]": "b24",
            "auth[domain]": "demo.bitrix24.ru",
        }
    )

    async def test_enrichment(self, http: httpx.AsyncClient) -> None:
        conn = Bitrix24Inbound(
            "b24",
            secret="b24",
            options={"rest_webhook_url": "https://b24.test/rest/1/SECRET/"},
            http=http,
        )
        with respx.mock:
            route = respx.get("https://b24.test/rest/1/SECRET/crm.lead.get.json").respond(
                json={
                    "result": {
                        "NAME": "Pavel",
                        "LAST_NAME": "Ivanov",
                        "PHONE": [{"VALUE": "+79001112233"}],
                        "EMAIL": [{"VALUE": "p@example.com"}],
                    }
                }
            )
            request = req(self.body)
            conn.verify(request, now=NOW)
            (event,) = (await conn.parse(request)).events
        assert route.calls.last.request.url.params["id"] == "77"
        assert event.type == "lead.created"
        assert event.contact.name == "Pavel Ivanov"
        assert event.contact.phone == "+79001112233"
        assert "application_token" not in json.dumps(event.raw)

    async def test_enrichment_failure_is_soft(self, http: httpx.AsyncClient) -> None:
        conn = Bitrix24Inbound(
            "b24", secret="b24", options={"rest_webhook_url": "https://b24.test/rest/x"}, http=http
        )
        with respx.mock:
            respx.get(url__startswith="https://b24.test/").respond(500)
            (event,) = (await conn.parse(req(self.body))).events
        assert event.fields["enrichment"] == "failed"

    async def test_rejects(self, http: httpx.AsyncClient) -> None:
        conn = Bitrix24Inbound("b24", secret="other", options={}, http=http)
        with pytest.raises(SignatureError):
            conn.verify(req(self.body), now=NOW)
        with pytest.raises(PayloadError):
            await conn.parse(req("event=ONCRMLEADADD"))


class TestGeneric:
    async def test_signed_list(self, http: httpx.AsyncClient) -> None:
        conn = GenericJsonInbound("p", secret="sec", options={"type_field": "event"}, http=http)
        body = json.dumps(
            [
                {"event": "order.paid", "id": "o1", "contact": {"name": "Ann"}, "data": {"x": 1}},
                {"id": "o2", "email": "b@example.com", "amount": 5},
            ]
        ).encode()
        request = req(
            body,
            content_type="application/json",
            headers={"X-Relay-Signature": sign_payload("sec", body, NOW)},
        )
        conn.verify(request, now=NOW + 5)
        first, second = (await conn.parse(request)).events
        assert (first.type, first.external_id, first.fields) == ("order.paid", "o1", {"x": 1})
        assert first.contact.name == "Ann"
        assert second.type == "custom" and second.contact.email == "b@example.com"
        assert second.fields == {"email": "b@example.com", "amount": 5}

    async def test_bad_payloads(self, http: httpx.AsyncClient) -> None:
        conn = GenericJsonInbound("p", secret=None, options={}, http=http)
        with pytest.raises(PayloadError):
            await conn.parse(req(b"{nope", content_type="application/json"))
        with pytest.raises(PayloadError):
            await conn.parse(req(b"[1, 2]", content_type="application/json"))
        with pytest.raises(SignatureError):
            GenericJsonInbound("p", secret="s", options={}, http=http).verify(
                req(b"{}", content_type="application/json"), now=NOW
            )


# -- outbound --------------------------------------------------------------------------------


class TestTelegram:
    options = {"bot_token": "123:SECRET", "chat_id": "-1", "api_base": "https://tg.test"}
    url = "https://tg.test/bot123:SECRET/sendMessage"

    async def test_success(self, http: httpx.AsyncClient) -> None:
        conn = TelegramConnector("tg", options=self.options, http=http)
        with respx.mock:
            route = respx.post(self.url).respond(json={"ok": True, "result": {"message_id": 7}})
            outcome = await conn.send({"text": "<b>hi</b>"}, CTX)
        body = json.loads(route.calls.last.request.content)
        assert body == {
            "chat_id": "-1",
            "text": "<b>hi</b>",
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
            "disable_notification": False,
        }
        assert outcome.result == {"message_id": 7}

    async def test_rate_limit_is_retryable_with_hint(self, http: httpx.AsyncClient) -> None:
        conn = TelegramConnector("tg", options=self.options, http=http)
        with respx.mock:
            respx.post(self.url).respond(
                429,
                json={
                    "ok": False,
                    "description": "Too Many Requests",
                    "parameters": {"retry_after": 17},
                },
            )
            with pytest.raises(DeliveryError) as info:
                await conn.send({"text": "x"}, CTX)
        assert info.value.retryable and info.value.retry_after == 17

    async def test_permanent_errors(self, http: httpx.AsyncClient) -> None:
        conn = TelegramConnector("tg", options=self.options, http=http)
        with respx.mock:
            respx.post(self.url).respond(400, json={"ok": False, "description": "chat not found"})
            with pytest.raises(DeliveryError) as info:
                await conn.send({"text": "x"}, CTX)
        assert not info.value.retryable
        with pytest.raises(DeliveryError, match="empty"):
            await conn.send({"text": "  "}, CTX)

    async def test_network_error_does_not_leak_token(self, http: httpx.AsyncClient) -> None:
        conn = TelegramConnector("tg", options=self.options, http=http)
        with respx.mock:
            respx.post(self.url).mock(side_effect=httpx.ConnectError("boom " + self.url))
            with pytest.raises(DeliveryError) as info:
                await conn.send({"text": "x"}, CTX)
        assert info.value.retryable and "SECRET" not in info.value.message

    def test_invalid_options(self, http: httpx.AsyncClient) -> None:
        with pytest.raises(ConfigError):
            TelegramConnector("tg", options={"chat_id": "1"}, http=http)


class TestWebhook:
    async def test_signed_delivery(self, http: httpx.AsyncClient) -> None:
        conn = WebhookConnector(
            "hook", options={"url": "https://hook.test/x", "secret": "whsec"}, http=http
        )
        with respx.mock:
            route = respx.post("https://hook.test/x").respond(204)
            await conn.send({"a": "б"}, CTX)
        sent = route.calls.last.request
        assert sent.headers["idempotency-key"] == "dlv_1"
        assert json.loads(sent.content) == {"a": "б"}
        verify_signature(
            "whsec",
            sent.content,
            sent.headers["x-relay-signature"],
            now=datetime.now(UTC).timestamp(),
        )

    @pytest.mark.parametrize(("status", "retryable"), [(503, True), (429, True), (422, False)])
    async def test_status_mapping(
        self, http: httpx.AsyncClient, status: int, retryable: bool
    ) -> None:
        conn = WebhookConnector("hook", options={"url": "https://hook.test/x"}, http=http)
        with respx.mock:
            respx.post("https://hook.test/x").respond(
                status, headers={"Retry-After": "30"}, text="nope"
            )
            with pytest.raises(DeliveryError) as info:
                await conn.send({}, CTX)
        assert info.value.retryable is retryable
        assert info.value.status_code == status and info.value.retry_after == 30


class TestAmoCrmLead:
    options = {
        "subdomain": "demo",
        "access_token": "amo-secret",
        "pipeline_id": 5,
        "tags": ["relay"],
    }

    async def test_complex_lead_and_note(self, http: httpx.AsyncClient) -> None:
        conn = AmoCrmLeadConnector("amo", options=self.options, http=http)
        with respx.mock:
            lead = respx.post("https://demo.amocrm.ru/api/v4/leads/complex").respond(
                json=[{"id": 100, "contact_id": 200}]
            )
            note = respx.post("https://demo.amocrm.ru/api/v4/leads/notes").respond(500)
            outcome = await conn.send(
                {
                    "name": "Сайт: заявка",
                    "price": "1500",
                    "contact": {"name": "Anna", "phone": "89001112233", "email": "a@b.c"},
                    "tags": ["tilda", ""],
                    "note": "call me",
                },
                CTX,
            )
        sent = json.loads(lead.calls.last.request.content)[0]
        assert lead.calls.last.request.headers["authorization"] == "Bearer amo-secret"
        assert sent["price"] == 1500 and sent["pipeline_id"] == 5
        contact = sent["_embedded"]["contacts"][0]
        assert contact["custom_fields_values"][0]["values"][0]["value"] == "+79001112233"
        assert sent["_embedded"]["tags"] == [{"name": "relay"}, {"name": "tilda"}]
        assert note.called
        # Lead exists -> success even though the note failed (no duplicate leads on retry).
        assert outcome.result == {"lead_id": 100, "contact_id": 200, "note_error": "HTTP 500"}

    async def test_unauthorized_is_permanent(self, http: httpx.AsyncClient) -> None:
        conn = AmoCrmLeadConnector("amo", options=self.options, http=http)
        with respx.mock:
            respx.post(url__startswith="https://demo.amocrm.ru").respond(401)
            with pytest.raises(DeliveryError) as info:
                await conn.send({"name": "x"}, CTX)
        assert not info.value.retryable
        with pytest.raises(DeliveryError, match="empty"):
            await conn.send({"name": ""}, CTX)


class TestGoogleSheets:
    async def test_append_with_static_token(self, http: httpx.AsyncClient) -> None:
        conn = GoogleSheetsConnector(
            "sheet",
            options={"spreadsheet_id": "S", "range": "Leads!A:C", "access_token": "tok"},
            http=http,
        )
        with respx.mock:
            route = respx.post(
                "https://sheets.googleapis.com/v4/spreadsheets/S/values/Leads%21A%3AC:append"
            ).respond(json={"updates": {"updatedRange": "Leads!A5:C5", "updatedRows": 1}})
            outcome = await conn.send({"values": ["a", None, 3]}, CTX)
        sent = route.calls.last.request
        assert json.loads(sent.content)["values"] == [["a", "", 3]]
        assert sent.url.params["valueInputOption"] == "USER_ENTERED"
        assert outcome.result == {"updated_range": "Leads!A5:C5", "rows": 1}

    async def test_service_account_jwt_flow(self, http: httpx.AsyncClient) -> None:
        key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        pem = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        ).decode()
        info = {
            "client_email": "relay@demo.iam.gserviceaccount.com",
            "private_key": pem,
            "token_uri": "https://oauth.test/token",
        }
        now = [1000.0]
        provider = ServiceAccountTokenProvider(info, http, clock=lambda: now[0])
        with respx.mock:
            token_route = respx.post("https://oauth.test/token").respond(
                json={"access_token": "ya29.token", "expires_in": 3600}
            )
            assert await provider.get() == "ya29.token"
            assert await provider.get() == "ya29.token"  # cached
            assert token_route.call_count == 1
            now[0] += 3590  # within the refresh margin
            await provider.get()
            assert token_route.call_count == 2
        form = parse_qs(token_route.calls.last.request.content.decode())
        claims = jwt.decode(
            form["assertion"][0],
            key.public_key(),
            algorithms=["RS256"],
            audience=info["token_uri"],
            options={"verify_exp": False, "verify_iat": False},
        )
        assert form["grant_type"] == ["urn:ietf:params:oauth:grant-type:jwt-bearer"]
        assert claims["iss"] == info["client_email"]

    async def test_401_invalidates_token_and_retries(self, http: httpx.AsyncClient) -> None:
        conn = GoogleSheetsConnector(
            "sheet", options={"spreadsheet_id": "S", "access_token": "tok"}, http=http
        )
        with respx.mock:
            respx.post(url__startswith="https://sheets.googleapis.com").respond(401)
            with pytest.raises(DeliveryError) as info:
                await conn.send({"rows": [["a"]]}, CTX)
        assert info.value.retryable
        with pytest.raises(DeliveryError, match="values"):
            await conn.send({"foo": 1}, CTX)

    def test_requires_auth(self, http: httpx.AsyncClient) -> None:
        with pytest.raises(ConfigError):
            GoogleSheetsConnector("s", options={"spreadsheet_id": "S"}, http=http)


class TestSmtp:
    options = {
        "host": "smtp.test",
        "port": 2525,
        "from_addr": "relay@shop.test",
        "to": ["m@shop.test"],
        "password": "mail-pass",
        "username": "relay",
    }

    async def test_message(self, http: httpx.AsyncClient) -> None:
        sent: list[Any] = []

        async def sender(message: Any, **kwargs: Any) -> None:
            sent.append((message, kwargs))

        conn = SmtpEmailConnector("mail", options=self.options, http=http, sender=sender)
        outcome = await conn.send({"subject": "Заказ\n42", "text": "Body", "html": "<p>x</p>"}, CTX)
        message, kwargs = sent[0]
        assert message["Subject"] == "Заказ 42" and message["To"] == "m@shop.test"
        assert message["X-Relay-Event-Id"] == "evt_1"
        assert kwargs["hostname"] == "smtp.test" and kwargs["password"] == "mail-pass"
        assert outcome.result and "dlv_1" in outcome.result["message_id"]

    @pytest.mark.parametrize(("code", "retryable"), [(451, True), (550, False)])
    async def test_reply_codes(self, http: httpx.AsyncClient, code: int, retryable: bool) -> None:
        async def sender(message: Any, **kwargs: Any) -> None:
            raise aiosmtplib.SMTPResponseException(code, "nope")

        conn = SmtpEmailConnector("mail", options=self.options, http=http, sender=sender)
        with pytest.raises(DeliveryError) as info:
            await conn.send({"subject": "s", "text": "t"}, CTX)
        assert info.value.retryable is retryable

    async def test_connection_error_and_validation(self, http: httpx.AsyncClient) -> None:
        async def sender(message: Any, **kwargs: Any) -> None:
            raise ConnectionRefusedError("refused")

        conn = SmtpEmailConnector("mail", options=self.options, http=http, sender=sender)
        with pytest.raises(DeliveryError) as info:
            await conn.send({"subject": "s", "text": "t"}, CTX)
        assert info.value.retryable
        with pytest.raises(DeliveryError, match="required"):
            await conn.send({"subject": "s"}, CTX)
