"""amoCRM account webhooks (``leads[add]``, ``leads[status]``, ``contacts[update]``...).

amoCRM sends ``x-www-form-urlencoded`` bodies in PHP bracket notation and may pack several
entities and actions into one request - each becomes a separate normalized event.
Account webhooks are not signed, so the endpoint URL carries a secret token
(``/webhooks/amocrm?token=...``) that is compared in constant time.
amoCRM disables a webhook after repeated slow/failed responses, which is why the API
answers ``202`` immediately and does the real work in the background worker.
"""

from __future__ import annotations

from typing import Any

from relay.connectors.base import InboundConnector, InboundRequest, InboundResult, inbound
from relay.domain import Contact, NormalizedEvent
from relay.errors import PayloadError, SignatureError
from relay.security import secure_compare

ENTITIES = {
    "leads": "lead",
    "contacts": "contact",
    "companies": "company",
    "customers": "customer",
    "tasks": "task",
}
ACTIONS = {
    "add": "created",
    "update": "updated",
    "delete": "deleted",
    "restore": "restored",
    "status": "status_changed",
    "responsible": "responsible_changed",
    "note": "note_added",
}


def _custom_fields(item: dict[str, Any]) -> tuple[dict[str, Any], dict[str, str]]:
    """Flatten ``custom_fields`` into ``{name: value}`` and collect PHONE / EMAIL codes."""
    flat: dict[str, Any] = {}
    codes: dict[str, str] = {}
    fields = item.get("custom_fields") or []
    if isinstance(fields, dict):
        fields = list(fields.values())
    for field in fields:
        if not isinstance(field, dict):
            continue
        values = field.get("values") or []
        if isinstance(values, dict):
            values = list(values.values())
        texts = [
            str(v.get("value")) if isinstance(v, dict) else str(v) for v in values if v is not None
        ]
        value: Any = texts[0] if len(texts) == 1 else texts
        name = str(field.get("name") or field.get("code") or field.get("id"))
        flat[name] = value
        code = str(field.get("code") or "").upper()
        if code in ("PHONE", "EMAIL") and texts:
            codes[code] = texts[0]
    return flat, codes


@inbound
class AmoCrmInbound(InboundConnector):
    kind = "amocrm"
    label = "amoCRM"

    def verify(self, request: InboundRequest, *, now: float) -> None:
        if self.secret is None:
            return
        provided = request.query.get("token") or request.header("x-relay-token")
        if not secure_compare(provided, self.secret):
            raise SignatureError("invalid amoCRM webhook token")

    async def parse(self, request: InboundRequest) -> InboundResult:
        data = request.payload()
        account = data.get("account") if isinstance(data.get("account"), dict) else {}
        events: list[NormalizedEvent] = []
        for entity_key, entity in ENTITIES.items():
            actions = data.get(entity_key)
            if not isinstance(actions, dict):
                continue
            for action, items in actions.items():
                if isinstance(items, dict):
                    items = [items]
                if not isinstance(items, list):
                    continue
                for item in items:
                    if isinstance(item, dict):
                        events.append(self._event(entity, str(action), item, account or {}))
        if not events:
            raise PayloadError("no amoCRM entities found in the webhook body")
        return InboundResult(events=events)

    @staticmethod
    def _event(
        entity: str, action: str, item: dict[str, Any], account: dict[str, Any]
    ) -> NormalizedEvent:
        custom, codes = _custom_fields(item)
        entity_id = item.get("id")
        version = item.get("last_modified") or item.get("updated_at") or item.get("status_id") or ""
        fields: dict[str, Any] = {
            key: value for key, value in item.items() if key != "custom_fields"
        }
        fields["entity"] = entity
        fields["custom"] = custom
        fields["account"] = {k: account.get(k) for k in ("id", "subdomain") if account.get(k)}
        contact = Contact()
        if entity == "contact":
            contact = Contact(
                name=item.get("name"), phone=codes.get("PHONE"), email=codes.get("EMAIL")
            )
        return NormalizedEvent(
            type=f"{entity}.{ACTIONS.get(action, action)}",
            external_id=f"{entity}:{entity_id}:{action}:{version}",
            contact=contact,
            fields=fields,
            raw=item,
        )
