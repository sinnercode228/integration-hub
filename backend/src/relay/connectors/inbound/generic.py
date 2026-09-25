"""Generic JSON webhooks signed with HMAC-SHA256 (``X-Relay-Signature: t=...,v1=...``).

Suits any in-house system or partner: the body can be a single object or a list of objects.
Field names are configurable, e.g. ``options: {type_field: event, id_field: uuid}``.
"""

from __future__ import annotations

from typing import Any

from relay.connectors.base import InboundConnector, InboundRequest, InboundResult, inbound
from relay.domain import Contact, NormalizedEvent
from relay.errors import PayloadError
from relay.security import SIGNATURE_HEADER, verify_signature


@inbound
class GenericJsonInbound(InboundConnector):
    kind = "generic"
    label = "Generic JSON (HMAC)"

    def verify(self, request: InboundRequest, *, now: float) -> None:
        if self.secret is None:
            return
        verify_signature(
            self.secret,
            request.body,
            request.header(SIGNATURE_HEADER),
            now=now,
            tolerance=int(self.options.get("tolerance_seconds", 300)),
        )

    async def parse(self, request: InboundRequest) -> InboundResult:
        data = request.json()
        items = data if isinstance(data, list) else [data]
        if not items or not all(isinstance(item, dict) for item in items):
            raise PayloadError("expected a JSON object or a list of objects")
        return InboundResult(events=[self._event(item) for item in items])

    def _event(self, item: dict[str, Any]) -> NormalizedEvent:
        type_field = str(self.options.get("type_field", "type"))
        id_field = str(self.options.get("id_field", "id"))
        event_type = item.get(type_field) or self.options.get("default_type") or "custom"
        external_id = item.get(id_field)
        nested = item.get("contact")
        contact_data: dict[str, Any] = nested if isinstance(nested, dict) else item
        contact = Contact(
            name=_str(contact_data.get("name")),
            phone=_str(contact_data.get("phone")),
            email=_str(contact_data.get("email")),
        )
        body = item.get("data")
        fields = (
            dict(body)
            if isinstance(body, dict)
            else {k: v for k, v in item.items() if k not in (type_field, id_field, "contact")}
        )
        return NormalizedEvent(
            type=str(event_type),
            external_id=str(external_id) if external_id is not None else None,
            contact=contact,
            fields=fields,
            raw=item,
        )


def _str(value: Any) -> str | None:
    return None if value in (None, "") else str(value)
