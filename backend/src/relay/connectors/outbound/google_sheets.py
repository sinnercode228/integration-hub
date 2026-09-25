"""Google Sheets: append rows via ``spreadsheets.values.append``.

Authentication uses a service account (OAuth 2.0 JWT bearer flow, RS256) implemented with
PyJWT - no heavy Google SDK. Access tokens are cached until shortly before expiry and
dropped on ``401`` so the next attempt re-authenticates.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any, ClassVar, Literal, Protocol
from urllib.parse import quote

import httpx
import jwt
from pydantic import BaseModel, SecretStr, field_validator, model_validator

from relay.connectors.base import (
    DeliveryContext,
    DeliveryOutcome,
    OutboundConnector,
    outbound,
    parse_options,
    require_dict,
)
from relay.connectors.http import raise_for_status, send_request
from relay.errors import ConfigError, DeliveryError

SHEETS_SCOPE = "https://www.googleapis.com/auth/spreadsheets"
JWT_GRANT = "urn:ietf:params:oauth:grant-type:jwt-bearer"


class TokenProvider(Protocol):
    async def get(self) -> str: ...

    def invalidate(self) -> None: ...


class StaticTokenProvider:
    def __init__(self, token: str) -> None:
        self._token = token

    async def get(self) -> str:
        return self._token

    def invalidate(self) -> None:
        return None


class ServiceAccountTokenProvider:
    def __init__(
        self,
        info: Mapping[str, Any],
        http: httpx.AsyncClient,
        *,
        scope: str = SHEETS_SCOPE,
        clock: Callable[[], float] = time.time,
    ) -> None:
        missing = [k for k in ("client_email", "private_key") if not info.get(k)]
        if missing:
            raise ConfigError(f"service account JSON is missing: {', '.join(missing)}")
        self._info = dict(info)
        self._http = http
        self._scope = scope
        self._clock = clock
        self._token: str | None = None
        self._expires_at = 0.0
        self._lock = asyncio.Lock()

    @property
    def token_uri(self) -> str:
        return str(self._info.get("token_uri") or "https://oauth2.googleapis.com/token")

    def _assertion(self, now: int) -> str:
        claims = {
            "iss": self._info["client_email"],
            "scope": self._scope,
            "aud": self.token_uri,
            "iat": now,
            "exp": now + 3600,
        }
        headers = (
            {"kid": self._info["private_key_id"]} if self._info.get("private_key_id") else None
        )
        return jwt.encode(claims, self._info["private_key"], algorithm="RS256", headers=headers)

    async def get(self) -> str:
        async with self._lock:
            now = self._clock()
            if self._token and now < self._expires_at - 60:
                return self._token
            response = await send_request(
                self._http,
                "POST",
                self.token_uri,
                "google-oauth",
                data={"grant_type": JWT_GRANT, "assertion": self._assertion(int(now))},
            )
            raise_for_status(response, "google-oauth")
            data = response.json()
            self._token = str(data["access_token"])
            self._expires_at = now + float(data.get("expires_in", 3600))
            return self._token

    def invalidate(self) -> None:
        self._token = None
        self._expires_at = 0.0


class GoogleSheetsOptions(BaseModel):
    spreadsheet_id: str
    range: str = "Sheet1!A:Z"
    value_input_option: Literal["RAW", "USER_ENTERED"] = "USER_ENTERED"
    service_account_json: SecretStr | None = None
    service_account_file: Path | None = None
    access_token: SecretStr | None = None
    api_base: str = "https://sheets.googleapis.com"

    @field_validator("service_account_json", "service_account_file", "access_token", mode="before")
    @classmethod
    def _empty_is_none(cls, value: Any) -> Any:
        return None if value == "" else value

    @model_validator(mode="after")
    def _one_auth_method(self) -> GoogleSheetsOptions:
        if not (self.service_account_json or self.service_account_file or self.access_token):
            raise ValueError(
                "set one of service_account_json, service_account_file or access_token"
            )
        return self


@outbound
class GoogleSheetsConnector(OutboundConnector):
    kind = "google_sheets"
    label = "Google Sheets"
    default_template: ClassVar[Any] = {
        "values": [
            "{{ received_at | date }}",
            "{{ source }}",
            "{{ type }}",
            "{{ contact.name }}",
            "{{ contact.phone | phone }}",
            "{{ contact.email }}",
        ]
    }

    def __init__(
        self,
        name: str,
        *,
        options: Mapping[str, Any],
        http: httpx.AsyncClient,
        token_provider: TokenProvider | None = None,
    ) -> None:
        super().__init__(name, options=options, http=http)
        self.options = parse_options(GoogleSheetsOptions, options, f"destination {name!r}")
        self.tokens = token_provider or self._make_provider()

    def _make_provider(self) -> TokenProvider:
        opts = self.options
        try:
            if opts.service_account_json is not None:
                info = json.loads(opts.service_account_json.get_secret_value())
            elif opts.service_account_file is not None:
                info = json.loads(opts.service_account_file.read_text(encoding="utf-8"))
            else:
                assert opts.access_token is not None
                return StaticTokenProvider(opts.access_token.get_secret_value())
        except (OSError, json.JSONDecodeError) as exc:
            raise ConfigError(f"destination {self.name!r}: cannot load service account") from exc
        return ServiceAccountTokenProvider(info, self.http)

    async def send(self, payload: Any, context: DeliveryContext) -> DeliveryOutcome:
        data = require_dict(payload, self.kind)
        rows = data.get("rows")
        if rows is None and data.get("values") is not None:
            rows = [data["values"]]
        if not isinstance(rows, list) or not all(isinstance(r, list) for r in rows):
            raise DeliveryError(
                "google_sheets: payload needs 'values' (one row) or 'rows'", retryable=False
            )
        rows = [["" if cell is None else cell for cell in row] for row in rows]
        opts = self.options
        url = (
            f"{opts.api_base.rstrip('/')}/v4/spreadsheets/{opts.spreadsheet_id}"
            f"/values/{quote(opts.range, safe='')}:append"
        )
        token = await self.tokens.get()
        response = await send_request(
            self.http,
            "POST",
            url,
            self.kind,
            token,
            params={"valueInputOption": opts.value_input_option, "insertDataOption": "INSERT_ROWS"},
            headers={"Authorization": f"Bearer {token}"},
            json={"majorDimension": "ROWS", "values": rows},
        )
        if response.status_code == 401:
            self.tokens.invalidate()
            raise DeliveryError(
                "google_sheets: HTTP 401 - token rejected", retryable=True, status_code=401
            )
        raise_for_status(response, self.kind, token)
        updates = response.json().get("updates", {})
        return DeliveryOutcome(
            status_code=response.status_code,
            result={
                "updated_range": updates.get("updatedRange"),
                "rows": updates.get("updatedRows"),
            },
        )
