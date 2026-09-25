"""Generic outbound webhook: JSON body, optional HMAC signature, idempotency header.

Receivers get ``Idempotency-Key: <delivery id>`` so they can safely deduplicate retries,
and ``X-Relay-Signature: t=...,v1=...`` (the same scheme Relay accepts inbound).
"""

from __future__ import annotations

import json
import time
from collections.abc import Mapping
from typing import Any, ClassVar, Literal

import httpx
from pydantic import BaseModel, Field, SecretStr

from relay import __version__
from relay.connectors.base import (
    DeliveryContext,
    DeliveryOutcome,
    OutboundConnector,
    outbound,
    parse_options,
)
from relay.connectors.http import raise_for_status, send_request
from relay.security import sign_payload


class WebhookOptions(BaseModel):
    url: str = Field(pattern=r"^https?://")
    method: Literal["POST", "PUT", "PATCH"] = "POST"
    headers: dict[str, str] = Field(default_factory=dict)
    secret: SecretStr | None = None


@outbound
class WebhookConnector(OutboundConnector):
    kind = "webhook"
    label = "Webhook (HTTP)"
    default_template: ClassVar[Any] = "{{ event }}"

    def __init__(self, name: str, *, options: Mapping[str, Any], http: httpx.AsyncClient) -> None:
        super().__init__(name, options=options, http=http)
        self.options = parse_options(WebhookOptions, options, f"destination {name!r}")

    def secrets(self) -> tuple[str | None, ...]:
        secret = self.options.secret
        return (secret.get_secret_value() if secret else None,)

    async def send(self, payload: Any, context: DeliveryContext) -> DeliveryOutcome:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), default=str).encode()
        headers = {
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": f"Relay/{__version__}",
            "Idempotency-Key": context.delivery_id,
            "X-Relay-Event-Id": context.event_id,
            "X-Relay-Attempt": str(context.attempt),
            **self.options.headers,
        }
        if self.options.secret is not None:
            headers["X-Relay-Signature"] = sign_payload(
                self.options.secret.get_secret_value(), body, int(time.time())
            )
        response = await send_request(
            self.http,
            self.options.method,
            self.options.url,
            self.kind,
            content=body,
            headers=headers,
        )
        raise_for_status(response, self.kind)
        return DeliveryOutcome(
            status_code=response.status_code, result={"status": response.status_code}
        )
