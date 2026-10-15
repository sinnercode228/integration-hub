"""amoCRM lead creation via ``POST /api/v4/leads/complex`` (lead + contact in one call).

A follow-up note (e.g. the form comment) is added with ``/api/v4/leads/notes``. If the lead
was created but the note failed, the delivery is still *successful*: retrying would create
a duplicate lead, so the note error is recorded in the result instead. That holds for every
kind of failure, and the note gives up before the attempt's deadline, so the worker always
gets the lead id back.
"""

from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, Field, SecretStr

from relay.connectors.base import (
    DeliveryContext,
    DeliveryOutcome,
    OutboundConnector,
    outbound,
    parse_options,
    require_dict,
)
from relay.connectors.http import raise_for_status, send_request
from relay.errors import DeliveryError
from relay.log import get_logger
from relay.templating import normalize_phone

log = get_logger(__name__)

# The note stops this long before the attempt's deadline, so that the result with the lead id
# reaches the worker before the attempt is cut off.
NOTE_DEADLINE_MARGIN_SECONDS = 0.5


class AmoCrmLeadOptions(BaseModel):
    subdomain: str = Field(pattern=r"^[a-z0-9-]+$")
    access_token: SecretStr
    base_domain: str = "amocrm.ru"
    pipeline_id: int | None = None
    status_id: int | None = None
    responsible_user_id: int | None = None
    tags: list[str] = Field(default_factory=list)
    note_timeout_seconds: float = Field(default=5.0, gt=0)
    base_url: str | None = None  # override for tests / proxies


def _contact_block(contact: Mapping[str, Any]) -> dict[str, Any] | None:
    fields: list[dict[str, Any]] = []
    phone = normalize_phone(contact.get("phone")) if contact.get("phone") else None
    if phone:
        fields.append({"field_code": "PHONE", "values": [{"value": phone, "enum_code": "WORK"}]})
    if contact.get("email"):
        fields.append(
            {
                "field_code": "EMAIL",
                "values": [{"value": str(contact["email"]), "enum_code": "WORK"}],
            }
        )
    name = str(contact.get("name") or "").strip()
    if not name and not fields:
        return None
    block: dict[str, Any] = {"name": name or phone or str(contact.get("email"))}
    if fields:
        block["custom_fields_values"] = fields
    return block


@outbound
class AmoCrmLeadConnector(OutboundConnector):
    kind = "amocrm_lead"
    label = "amoCRM: create lead"
    default_template: ClassVar[Any] = {
        "name": "{{ fields.form_name | default('Заявка с сайта') }}",
        "price": "{{ fields.order.amount | int }}",
        "contact": {
            "name": "{{ contact.name }}",
            "phone": "{{ contact.phone }}",
            "email": "{{ contact.email }}",
        },
        "tags": ["{{ source }}"],
        "note": "{{ fields.comment | default('') }}",
    }

    def __init__(self, name: str, *, options: Mapping[str, Any], http: httpx.AsyncClient) -> None:
        super().__init__(name, options=options, http=http)
        self.options = parse_options(AmoCrmLeadOptions, options, f"destination {name!r}")

    @property
    def base_url(self) -> str:
        opts = self.options
        return (opts.base_url or f"https://{opts.subdomain}.{opts.base_domain}").rstrip("/")

    def secrets(self) -> tuple[str | None, ...]:
        return (self.options.access_token.get_secret_value(),)

    def build_lead(self, data: Mapping[str, Any]) -> dict[str, Any]:
        opts = self.options
        name = str(data.get("name") or "").strip()
        if not name:
            raise DeliveryError("amocrm_lead: rendered 'name' is empty", retryable=False)
        lead: dict[str, Any] = {"name": name[:255]}
        try:
            price = int(data.get("price") or 0)
        except (TypeError, ValueError):
            price = 0
        if price:
            lead["price"] = price
        for key in ("pipeline_id", "status_id", "responsible_user_id"):
            value = data.get(key) or getattr(opts, key)
            if value:
                lead[key] = int(value)
        custom = data.get("custom_fields")
        if isinstance(custom, Mapping) and custom:
            lead["custom_fields_values"] = [
                {"field_id": int(field_id), "values": [{"value": value}]}
                for field_id, value in custom.items()
                if value not in (None, "")
            ]
        embedded: dict[str, Any] = {}
        contact = data.get("contact")
        if isinstance(contact, Mapping):
            block = _contact_block(contact)
            if block:
                embedded["contacts"] = [block]
        tags = [*opts.tags, *[t for t in data.get("tags") or [] if t]]
        if tags:
            embedded["tags"] = [{"name": str(tag)} for tag in dict.fromkeys(tags)]
        if embedded:
            lead["_embedded"] = embedded
        return lead

    async def send(self, payload: Any, context: DeliveryContext) -> DeliveryOutcome:
        data = require_dict(payload, self.kind)
        lead = self.build_lead(data)
        token = self.options.access_token.get_secret_value()
        headers = {"Authorization": f"Bearer {token}"}
        response = await send_request(
            self.http,
            "POST",
            f"{self.base_url}/api/v4/leads/complex",
            self.kind,
            token,
            headers=headers,
            json=[lead],
        )
        if response.status_code == 401:
            raise DeliveryError(
                "amocrm_lead: HTTP 401 - access token is invalid or expired",
                retryable=False,
                status_code=401,
            )
        raise_for_status(response, self.kind, token)
        created = response.json()
        first = created[0] if isinstance(created, list) and created else {}
        result: dict[str, Any] = {"lead_id": first.get("id"), "contact_id": first.get("contact_id")}

        note = str(data.get("note") or "").strip()
        if note and result["lead_id"]:
            note_error = await self.add_note(result["lead_id"], note, token, context)
            if note_error:
                result["note_error"] = note_error
        return DeliveryOutcome(status_code=response.status_code, result=result)

    async def add_note(
        self, lead_id: Any, text: str, token: str, context: DeliveryContext
    ) -> str | None:
        """Add a note to a lead that already exists. Returns why it failed, or ``None``."""
        budget = self.options.note_timeout_seconds
        if context.deadline is not None:
            left = context.deadline - asyncio.get_running_loop().time()
            budget = min(budget, left - NOTE_DEADLINE_MARGIN_SECONDS)
        if budget <= 0:
            return "not sent: the attempt had no time left"
        try:
            async with asyncio.timeout(budget):
                response = await send_request(
                    self.http,
                    "POST",
                    f"{self.base_url}/api/v4/leads/notes",
                    self.kind,
                    token,
                    headers={"Authorization": f"Bearer {token}"},
                    json=[{"entity_id": lead_id, "note_type": "common", "params": {"text": text}}],
                )
        except TimeoutError:
            return f"timeout ({budget:.1f} s)"
        except DeliveryError as exc:
            return exc.message.removeprefix(f"{self.kind}: ")
        except Exception as exc:
            # Whatever it was, the lead exists, and a retry would create it again.
            log.exception("amocrm.note_failed", destination=self.name, lead_id=lead_id)
            return f"unexpected error: {type(exc).__name__}"
        if not response.is_success:
            return f"HTTP {response.status_code}"
        return None
