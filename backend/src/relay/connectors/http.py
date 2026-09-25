"""HTTP helpers shared by outbound connectors: one place decides what is retryable."""

from __future__ import annotations

from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

from relay.errors import DeliveryError
from relay.security import redact

RETRYABLE_STATUS = frozenset({408, 409, 425, 429, 500, 502, 503, 504})


def retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("retry-after")
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        moment = parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
    if moment.tzinfo is None:
        moment = moment.replace(tzinfo=UTC)
    return max(0.0, (moment - datetime.now(UTC)).total_seconds())


def _short_body(response: httpx.Response, limit: int = 300) -> str:
    try:
        text = response.text
    except (UnicodeDecodeError, httpx.ResponseNotRead):
        return ""
    text = " ".join(text.split())
    return text[:limit]


def raise_for_status(
    response: httpx.Response, connector: str, *secrets: str | None, detail: str | None = None
) -> None:
    """Map an HTTP response to success / retryable failure / permanent failure."""
    if response.is_success:
        return
    status = response.status_code
    message = f"{connector}: HTTP {status}"
    extra = detail if detail is not None else _short_body(response)
    if extra:
        message = f"{message} - {extra}"
    raise DeliveryError(
        redact(message, *secrets),
        retryable=status in RETRYABLE_STATUS or status >= 500,
        status_code=status,
        retry_after=retry_after_seconds(response),
    )


async def send_request(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    connector: str,
    *secrets: str | None,
    **kwargs: Any,
) -> httpx.Response:
    """Perform a request; network-level failures become retryable ``DeliveryError``s."""
    try:
        return await client.request(method, url, **kwargs)
    except httpx.TimeoutException as exc:
        raise DeliveryError(f"{connector}: timeout ({type(exc).__name__})", retryable=True) from exc
    except httpx.TransportError as exc:
        text = redact(f"{connector}: network error ({type(exc).__name__}: {exc})", *secrets)
        raise DeliveryError(text, retryable=True) from exc
