"""Connector contracts and the plugin registry.

Adding an integration means writing one class and decorating it with ``@inbound`` or
``@outbound``; the pipeline, retries, metrics and dashboard work for it automatically.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, ClassVar

import httpx
from pydantic import BaseModel, ValidationError

from relay.domain import NormalizedEvent
from relay.errors import ConfigError, DeliveryError, PayloadError
from relay.forms import parse_form


@dataclass(frozen=True, slots=True)
class InboundRequest:
    body: bytes
    headers: Mapping[str, str]
    query: Mapping[str, str]
    content_type: str
    received_at: datetime

    def header(self, name: str) -> str | None:
        return self.headers.get(name.lower())

    @property
    def is_json(self) -> bool:
        return "json" in self.content_type.lower()

    def json(self) -> Any:
        try:
            return json.loads(self.body or b"null")
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise PayloadError(f"body is not valid JSON: {exc}") from exc

    def form(self) -> dict[str, Any]:
        return parse_form(self.body)

    def payload(self) -> dict[str, Any]:
        """JSON object or form fields, depending on the content type."""
        if self.is_json:
            data = self.json()
            if not isinstance(data, dict):
                raise PayloadError("expected a JSON object")
            return data
        return self.form()


@dataclass(slots=True)
class InboundResult:
    events: list[NormalizedEvent] = field(default_factory=list)
    ping: bool = False


@dataclass(slots=True)
class DeliveryOutcome:
    status_code: int | None = None
    result: dict[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class DeliveryContext:
    delivery_id: str
    event_id: str
    attempt: int


def parse_options[OptionsT: BaseModel](
    model: type[OptionsT], options: Mapping[str, Any], where: str
) -> OptionsT:
    try:
        return model.model_validate(dict(options))
    except ValidationError as exc:
        raise ConfigError(f"{where}: invalid options\n{exc}") from exc


class InboundConnector(ABC):
    kind: ClassVar[str]
    label: ClassVar[str]

    def __init__(
        self,
        source_id: str,
        *,
        secret: str | None,
        options: Mapping[str, Any],
        http: httpx.AsyncClient,
    ) -> None:
        self.source_id = source_id
        self.secret = secret
        self.options = dict(options)
        self.http = http

    @abstractmethod
    def verify(self, request: InboundRequest, *, now: float) -> None:
        """Raise :class:`~relay.errors.SignatureError` if the request is not authentic."""

    @abstractmethod
    async def parse(self, request: InboundRequest) -> InboundResult:
        """Turn a verified request into zero or more normalized events."""


class OutboundConnector(ABC):
    kind: ClassVar[str]
    label: ClassVar[str]
    default_template: ClassVar[Any]
    escape_fields: ClassVar[Mapping[str, Callable[[str], str]]] = {}

    def __init__(self, name: str, *, options: Mapping[str, Any], http: httpx.AsyncClient) -> None:
        self.name = name
        self.http = http

    @abstractmethod
    async def send(self, payload: Any, context: DeliveryContext) -> DeliveryOutcome:
        """Deliver one payload. Raise :class:`~relay.errors.DeliveryError` on failure."""

    def secrets(self) -> tuple[str | None, ...]:
        """Secret values to scrub from error messages."""
        return ()

    async def aclose(self) -> None:
        return None


INBOUND: dict[str, type[InboundConnector]] = {}
OUTBOUND: dict[str, type[OutboundConnector]] = {}


def inbound[InT: type[InboundConnector]](cls: InT) -> InT:
    INBOUND[cls.kind] = cls
    return cls


def outbound[OutT: type[OutboundConnector]](cls: OutT) -> OutT:
    OUTBOUND[cls.kind] = cls
    return cls


def require_dict(payload: Any, connector: str) -> dict[str, Any]:
    if not isinstance(payload, dict):
        raise DeliveryError(f"{connector}: rendered payload must be an object", retryable=False)
    return payload
