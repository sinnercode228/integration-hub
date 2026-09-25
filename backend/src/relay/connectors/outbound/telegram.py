"""Telegram Bot API notifications (``sendMessage``).

* User data is HTML-escaped automatically (``escape_fields``) while the template itself may
  use ``<b>``/``<i>`` - so a lead named ``<script>`` cannot break the message markup.
* ``429 Too Many Requests`` carries ``parameters.retry_after``; the worker honours it.
* ``400``/``403`` (chat not found, bot blocked) are permanent - retrying will not help.
"""

from __future__ import annotations

import html
from collections.abc import Callable, Mapping
from typing import Any, ClassVar, Literal

import httpx
from pydantic import BaseModel, SecretStr

from relay.connectors.base import (
    DeliveryContext,
    DeliveryOutcome,
    OutboundConnector,
    outbound,
    parse_options,
    require_dict,
)
from relay.connectors.http import send_request
from relay.errors import DeliveryError

MAX_MESSAGE_LENGTH = 4096


class TelegramOptions(BaseModel):
    bot_token: SecretStr
    chat_id: str
    message_thread_id: int | None = None
    parse_mode: Literal["HTML"] | None = "HTML"
    disable_web_page_preview: bool = True
    disable_notification: bool = False
    api_base: str = "https://api.telegram.org"


@outbound
class TelegramConnector(OutboundConnector):
    kind = "telegram"
    label = "Telegram"
    default_template: ClassVar[Any] = {
        "text": (
            "<b>{{ type }}</b> · {{ source }}\n"
            "{{ contact.name | default('Без имени') }}\n"
            "{{ contact.phone | phone | default('') }} {{ contact.email | default('') }}"
        )
    }
    escape_fields: ClassVar[Mapping[str, Callable[[str], str]]] = {"text": html.escape}

    def __init__(self, name: str, *, options: Mapping[str, Any], http: httpx.AsyncClient) -> None:
        super().__init__(name, options=options, http=http)
        self.options = parse_options(TelegramOptions, options, f"destination {name!r}")

    def secrets(self) -> tuple[str | None, ...]:
        return (self.options.bot_token.get_secret_value(),)

    async def send(self, payload: Any, context: DeliveryContext) -> DeliveryOutcome:
        data = require_dict(payload, self.kind)
        text = str(data.get("text") or "").strip()
        if not text:
            raise DeliveryError("telegram: rendered 'text' is empty", retryable=False)
        if len(text) > MAX_MESSAGE_LENGTH:
            text = text[: MAX_MESSAGE_LENGTH - 1] + "…"
        opts = self.options
        body: dict[str, Any] = {
            "chat_id": data.get("chat_id") or opts.chat_id,
            "text": text,
            "disable_web_page_preview": opts.disable_web_page_preview,
            "disable_notification": opts.disable_notification,
        }
        if opts.parse_mode:
            body["parse_mode"] = opts.parse_mode
        if opts.message_thread_id is not None:
            body["message_thread_id"] = opts.message_thread_id

        token = opts.bot_token.get_secret_value()
        url = f"{opts.api_base.rstrip('/')}/bot{token}/sendMessage"
        response = await send_request(self.http, "POST", url, self.kind, token, json=body)
        try:
            answer = response.json()
        except ValueError:
            answer = {}
        if response.is_success and answer.get("ok", True):
            message_id = (answer.get("result") or {}).get("message_id")
            return DeliveryOutcome(
                status_code=response.status_code, result={"message_id": message_id}
            )

        description = str(answer.get("description") or response.reason_phrase)
        retry_after = (answer.get("parameters") or {}).get("retry_after")
        status = response.status_code
        raise DeliveryError(
            f"telegram: HTTP {status} - {description}",
            retryable=status == 429 or status >= 500,
            status_code=status,
            retry_after=float(retry_after) if retry_after is not None else None,
        )
