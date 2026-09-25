"""E-mail notifications over SMTP (aiosmtplib, STARTTLS or implicit TLS).

SMTP reply codes are mapped to retry semantics: ``4xx`` is temporary (greylisting, mailbox
busy), ``5xx`` is permanent (bad recipient, rejected content).
"""

from __future__ import annotations

import html
from collections.abc import Awaitable, Callable, Mapping
from email.message import EmailMessage
from email.utils import formatdate, make_msgid
from typing import Any, ClassVar

import aiosmtplib
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
from relay.errors import DeliveryError

Sender = Callable[..., Awaitable[Any]]


class SmtpOptions(BaseModel):
    host: str
    port: int = 587
    username: str | None = None
    password: SecretStr | None = None
    from_addr: str
    to: list[str] = Field(min_length=1)
    start_tls: bool = True
    use_tls: bool = False
    timeout_seconds: float = 15.0


@outbound
class SmtpEmailConnector(OutboundConnector):
    kind = "smtp"
    label = "E-mail (SMTP)"
    default_template: ClassVar[Any] = {
        "subject": "[Relay] {{ type }} · {{ source }}",
        "text": (
            "Событие: {{ type }}\nИсточник: {{ source }}\n"
            "Имя: {{ contact.name | default('—') }}\n"
            "Телефон: {{ contact.phone | phone | default('—') }}\n"
            "E-mail: {{ contact.email | default('—') }}\n\n"
            "ID события: {{ id }}"
        ),
    }
    escape_fields: ClassVar[Mapping[str, Callable[[str], str]]] = {"html": html.escape}

    def __init__(
        self,
        name: str,
        *,
        options: Mapping[str, Any],
        http: httpx.AsyncClient,
        sender: Sender | None = None,
    ) -> None:
        super().__init__(name, options=options, http=http)
        self.options = parse_options(SmtpOptions, options, f"destination {name!r}")
        self._send = sender or aiosmtplib.send

    def secrets(self) -> tuple[str | None, ...]:
        password = self.options.password
        return (password.get_secret_value() if password else None,)

    def build_message(self, data: Mapping[str, Any], context: DeliveryContext) -> EmailMessage:
        subject = str(data.get("subject") or "").strip()
        text = str(data.get("text") or "").strip()
        if not subject or not (text or data.get("html")):
            raise DeliveryError(
                "smtp: 'subject' and 'text' (or 'html') are required", retryable=False
            )
        recipients = data.get("to") or self.options.to
        if isinstance(recipients, str):
            recipients = [recipients]
        message = EmailMessage()
        message["From"] = self.options.from_addr
        message["To"] = ", ".join(str(r) for r in recipients)
        message["Subject"] = subject.replace("\n", " ")
        message["Date"] = formatdate(localtime=False)
        domain = self.options.from_addr.rpartition("@")[2] or "relay.local"
        message["Message-ID"] = make_msgid(idstring=context.delivery_id, domain=domain)
        message["X-Relay-Event-Id"] = context.event_id
        message.set_content(text or "See the HTML version of this message.")
        if data.get("html"):
            message.add_alternative(str(data["html"]), subtype="html")
        return message

    async def send(self, payload: Any, context: DeliveryContext) -> DeliveryOutcome:
        data = require_dict(payload, self.kind)
        message = self.build_message(data, context)
        opts = self.options
        try:
            await self._send(
                message,
                hostname=opts.host,
                port=opts.port,
                username=opts.username,
                password=opts.password.get_secret_value() if opts.password else None,
                start_tls=opts.start_tls and not opts.use_tls,
                use_tls=opts.use_tls,
                timeout=opts.timeout_seconds,
            )
        except aiosmtplib.SMTPRecipientsRefused as exc:
            raise DeliveryError(
                f"smtp: recipients refused ({len(exc.recipients)})", retryable=False
            ) from exc
        except aiosmtplib.SMTPResponseException as exc:
            raise DeliveryError(
                f"smtp: {exc.code} {exc.message}", retryable=exc.code < 500, status_code=exc.code
            ) from exc
        except (aiosmtplib.SMTPException, OSError, TimeoutError) as exc:
            raise DeliveryError(f"smtp: {type(exc).__name__}: {exc}", retryable=True) from exc
        return DeliveryOutcome(result={"message_id": message["Message-ID"]})
