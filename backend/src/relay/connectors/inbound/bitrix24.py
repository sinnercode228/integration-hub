"""Bitrix24 outbound webhooks / event handlers (``ONCRMLEADADD``, ``ONCRMDEALUPDATE``...).

Bitrix24 sends only the entity id: ``event=ONCRMLEADADD&data[FIELDS][ID]=42&ts=...&
auth[application_token]=...``. The request is authenticated by ``application_token``.
If ``options.rest_webhook_url`` (an *incoming* webhook URL) is configured, Relay enriches
the event with ``crm.<entity>.get`` so routes can use the lead's name, phone and e-mail.
"""

from __future__ import annotations

from typing import Any

import httpx

from relay.connectors.base import InboundConnector, InboundRequest, InboundResult, inbound
from relay.domain import Contact, NormalizedEvent
from relay.errors import PayloadError, SignatureError
from relay.log import get_logger
from relay.security import redact, secure_compare

log = get_logger(__name__)

EVENTS = {
    "ONCRMLEADADD": ("lead", "created"),
    "ONCRMLEADUPDATE": ("lead", "updated"),
    "ONCRMLEADDELETE": ("lead", "deleted"),
    "ONCRMDEALADD": ("deal", "created"),
    "ONCRMDEALUPDATE": ("deal", "updated"),
    "ONCRMDEALDELETE": ("deal", "deleted"),
    "ONCRMCONTACTADD": ("contact", "created"),
    "ONCRMCONTACTUPDATE": ("contact", "updated"),
    "ONCRMCOMPANYADD": ("company", "created"),
    "ONCRMCOMPANYUPDATE": ("company", "updated"),
}
ENRICHABLE = {"lead", "deal", "contact", "company"}


def _multifield(entity: dict[str, Any], key: str) -> str | None:
    values = entity.get(key)
    if isinstance(values, list):
        for item in values:
            if isinstance(item, dict) and item.get("VALUE"):
                return str(item["VALUE"])
    return None


@inbound
class Bitrix24Inbound(InboundConnector):
    kind = "bitrix24"
    label = "Bitrix24"

    def verify(self, request: InboundRequest, *, now: float) -> None:
        if self.secret is None:
            return
        auth = request.payload().get("auth")
        provided = auth.get("application_token") if isinstance(auth, dict) else None
        if not secure_compare(provided if isinstance(provided, str) else None, self.secret):
            raise SignatureError("invalid Bitrix24 application_token")

    async def parse(self, request: InboundRequest) -> InboundResult:
        data = request.payload()
        event_name = str(data.get("event") or "").upper()
        payload = data.get("data") if isinstance(data.get("data"), dict) else {}
        fields_block = payload.get("FIELDS") if isinstance(payload, dict) else None
        entity_id = fields_block.get("ID") if isinstance(fields_block, dict) else None
        if not event_name or entity_id is None:
            raise PayloadError("Bitrix24 webhook must contain 'event' and 'data[FIELDS][ID]'")

        entity, action = EVENTS.get(event_name, ("bitrix", event_name.lower()))
        auth = data.get("auth") if isinstance(data.get("auth"), dict) else {}
        fields: dict[str, Any] = {
            "entity": entity,
            "entity_id": str(entity_id),
            "event": event_name,
            "domain": (auth or {}).get("domain"),
        }
        contact = Contact()
        if entity in ENRICHABLE and self.options.get("rest_webhook_url"):
            details = await self._fetch(entity, str(entity_id))
            if details is not None:
                fields["details"] = details
                name = " ".join(
                    str(details[k]) for k in ("NAME", "LAST_NAME") if details.get(k)
                ) or details.get("TITLE")
                contact = Contact(
                    name=name or None,
                    phone=_multifield(details, "PHONE"),
                    email=_multifield(details, "EMAIL"),
                )
            else:
                fields["enrichment"] = "failed"

        # Never keep auth tokens in stored raw payloads.
        raw = {k: v for k, v in data.items() if k != "auth"}
        raw["auth"] = {
            k: (auth or {}).get(k) for k in ("domain", "member_id") if (auth or {}).get(k)
        }
        event = NormalizedEvent(
            type=f"{entity}.{action}",
            external_id=f"{event_name}:{entity_id}:{data.get('ts', '')}",
            contact=contact,
            fields=fields,
            raw=raw,
        )
        return InboundResult(events=[event])

    async def _fetch(self, entity: str, entity_id: str) -> dict[str, Any] | None:
        base = str(self.options["rest_webhook_url"]).rstrip("/")
        url = f"{base}/crm.{entity}.get.json"
        try:
            response = await self.http.get(url, params={"id": entity_id}, timeout=5.0)
            response.raise_for_status()
            result = response.json().get("result")
        except (httpx.HTTPError, ValueError) as exc:
            log.warning(
                "bitrix24.enrichment_failed",
                source=self.source_id,
                error=redact(f"{type(exc).__name__}: {exc}", base),
            )
            return None
        return result if isinstance(result, dict) else None
