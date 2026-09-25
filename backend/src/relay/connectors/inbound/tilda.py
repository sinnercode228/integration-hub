"""Tilda forms and Tilda shop orders.

* Tilda posts ``application/x-www-form-urlencoded`` (or JSON if enabled) with the form fields
  as they are named in the editor (``Name``, ``Phone``, ``Email``...), plus ``formid``,
  ``formname``, ``tranid`` and optionally ``COOKIES`` / UTM fields.
* When a webhook is connected, Tilda sends ``test=test`` - we must answer ``ok``.
* Authentication: the API key configured in the Tilda webhook settings is sent as a field
  (``api_key`` by default); we compare it in constant time and never store it.
* Orders from the Tilda cart contain a ``payment`` field with a JSON document.
"""

from __future__ import annotations

import json
from typing import Any

from relay.connectors.base import InboundConnector, InboundRequest, InboundResult, inbound
from relay.domain import Contact, NormalizedEvent
from relay.errors import SignatureError
from relay.security import secure_compare

NAME_KEYS = ("name", "имя", "fio", "фио", "your_name")
PHONE_KEYS = ("phone", "телефон", "tel", "phone_number")
EMAIL_KEYS = ("email", "e-mail", "почта", "mail")
SERVICE_KEYS = {"formid", "formname", "tranid", "cookies", "payment", "test"}


def _pick(data: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    lowered = {str(k).lower(): v for k, v in data.items()}
    for key in keys:
        value = lowered.get(key)
        if value not in (None, ""):
            return str(value).strip()
    return None


def parse_cookies(value: str) -> dict[str, str]:
    utm: dict[str, str] = {}
    for chunk in value.split(";"):
        key, _, val = chunk.strip().partition("=")
        if key.lower().startswith("utm_") and val:
            utm[key.lower()] = val
    return utm


@inbound
class TildaInbound(InboundConnector):
    kind = "tilda"
    label = "Tilda"

    @property
    def key_field(self) -> str:
        return str(self.options.get("key_field", "api_key"))

    def verify(self, request: InboundRequest, *, now: float) -> None:
        if self.secret is None:
            return
        provided = request.header("x-api-key") or request.query.get(self.key_field)
        if provided is None:
            value = request.payload().get(self.key_field)
            provided = value if isinstance(value, str) else None
        if not secure_compare(provided, self.secret):
            raise SignatureError("invalid Tilda API key")

    async def parse(self, request: InboundRequest) -> InboundResult:
        data = request.payload()
        if data.get("test") == "test":
            return InboundResult(ping=True)
        data.pop(self.key_field, None)

        fields: dict[str, Any] = {}
        for key, value in data.items():
            normalized = str(key).strip().lower()
            if normalized in SERVICE_KEYS or normalized in NAME_KEYS + PHONE_KEYS + EMAIL_KEYS:
                continue
            fields[normalized] = value
        fields["form_id"] = data.get("formid")
        fields["form_name"] = data.get("formname")
        utm = {k: str(v) for k, v in fields.items() if k.startswith("utm_") and v}
        cookies = data.get("COOKIES") or data.get("cookies")
        if isinstance(cookies, str):
            utm = {**parse_cookies(cookies), **utm}
        if utm:
            fields["utm"] = utm

        event_type = "form.submitted"
        external_id = data.get("tranid")
        payment = data.get("payment")
        if isinstance(payment, str):
            try:
                payment = json.loads(payment)
            except json.JSONDecodeError:
                payment = None
        if isinstance(payment, dict):
            event_type = "order.created"
            fields["order"] = {
                "id": payment.get("orderid"),
                "amount": payment.get("amount"),
                "products": payment.get("products", []),
                "promocode": payment.get("promocode"),
            }
            external_id = external_id or payment.get("orderid")

        contact = Contact(
            name=_pick(data, NAME_KEYS),
            phone=_pick(data, PHONE_KEYS),
            email=_pick(data, EMAIL_KEYS),
        )
        event = NormalizedEvent(
            type=event_type,
            external_id=str(external_id) if external_id else None,
            contact=contact,
            fields=fields,
            raw=data,
        )
        return InboundResult(events=[event])
