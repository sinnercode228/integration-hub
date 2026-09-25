"""MoySklad stock proxy for storefront widgets.

The browser asks ``GET /api/stock?sku=A-1&sku=B-2``; Relay answers from a TTL cache or
queries MoySklad ``/entity/assortment`` with the server-side token. The token never
reaches the browser, and the widget gets only ``sku / available / status``.

* Fresh cache (``cache_ttl_seconds``) - served without an upstream call.
* Concurrent misses are coalesced by a lock (single flight) - a traffic spike on a product
  page produces one MoySklad request, not hundreds (MoySklad rate-limits per account).
* ``stale-if-error``: if MoySklad is down, values up to ``stale_ttl_seconds`` old are
  served and marked ``stale``.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

import httpx
from pydantic import BaseModel, Field, SecretStr

from relay.connectors.base import parse_options
from relay.connectors.http import raise_for_status, send_request
from relay.errors import DeliveryError, UpstreamUnavailableError
from relay.log import get_logger

log = get_logger(__name__)

FILTER_CHUNK = 25


class MoySkladOptions(BaseModel):
    token: SecretStr
    api_base: str = "https://api.moysklad.ru/api/remap/1.2"
    match_field: Literal["article", "code"] = "article"
    store_id: str | None = None
    cache_ttl_seconds: float = Field(default=60.0, ge=0)
    stale_ttl_seconds: float = Field(default=900.0, ge=0)
    low_stock_threshold: int = Field(default=3, ge=0)
    max_skus: int = Field(default=50, ge=1, le=200)


@dataclass(frozen=True, slots=True)
class StockLevel:
    sku: str
    found: bool
    available: int

    def status(self, low_threshold: int) -> str:
        if not self.found or self.available <= 0:
            return "out_of_stock"
        return "low" if self.available <= low_threshold else "in_stock"


@dataclass(frozen=True, slots=True)
class StockLookup:
    items: list[StockLevel]
    hits: int
    misses: int
    stale: bool


class MoySkladStockService:
    def __init__(
        self,
        options: Mapping[str, Any],
        http: httpx.AsyncClient,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.options = parse_options(MoySkladOptions, options, "stock")
        self._http = http
        self._clock = clock
        self._cache: dict[str, tuple[StockLevel, float]] = {}
        self._lock = asyncio.Lock()

    @property
    def low_stock_threshold(self) -> int:
        return self.options.low_stock_threshold

    def _fresh(self, sku: str, now: float) -> StockLevel | None:
        entry = self._cache.get(sku)
        if entry and now - entry[1] <= self.options.cache_ttl_seconds:
            return entry[0]
        return None

    async def lookup(self, skus: Sequence[str]) -> StockLookup:
        wanted = list(dict.fromkeys(s.strip() for s in skus if s.strip()))
        if len(wanted) > self.options.max_skus:
            raise ValueError(f"too many SKUs (max {self.options.max_skus})")
        now = self._clock()
        found: dict[str, StockLevel] = {}
        for sku in wanted:
            level = self._fresh(sku, now)
            if level is not None:
                found[sku] = level
        hits = len(found)
        missing = [sku for sku in wanted if sku not in found]
        stale = False
        if missing:
            async with self._lock:  # single flight: re-check after waiting for the lock
                now = self._clock()
                still_missing = []
                for sku in missing:
                    level = self._fresh(sku, now)
                    if level is not None:
                        found[sku] = level
                    else:
                        still_missing.append(sku)
                if still_missing:
                    try:
                        fetched = await self._fetch(still_missing)
                    except DeliveryError as exc:
                        found.update(self._stale_or_raise(still_missing, now, exc))
                        stale = True
                    else:
                        for sku in still_missing:
                            level = fetched.get(sku) or StockLevel(
                                sku=sku, found=False, available=0
                            )
                            self._cache[sku] = (level, now)
                            found[sku] = level
        return StockLookup(
            items=[found[sku] for sku in wanted], hits=hits, misses=len(missing), stale=stale
        )

    def _stale_or_raise(
        self, skus: list[str], now: float, error: DeliveryError
    ) -> dict[str, StockLevel]:
        result: dict[str, StockLevel] = {}
        for sku in skus:
            entry = self._cache.get(sku)
            if entry is None or now - entry[1] > self.options.stale_ttl_seconds:
                log.warning("stock.upstream_unavailable", error=error.message)
                raise UpstreamUnavailableError(
                    "stock service is temporarily unavailable"
                ) from error
            result[sku] = entry[0]
        log.warning("stock.serving_stale", skus=len(skus), error=error.message)
        return result

    async def _fetch(self, skus: list[str]) -> dict[str, StockLevel]:
        opts = self.options
        token = opts.token.get_secret_value()
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json;charset=utf-8",
            "Accept-Encoding": "gzip",
        }
        levels: dict[str, StockLevel] = {}
        for start in range(0, len(skus), FILTER_CHUNK):
            chunk = skus[start : start + FILTER_CHUNK]
            filters = [f"{opts.match_field}={sku}" for sku in chunk]
            if opts.store_id:
                filters.append(f"stockStore={opts.api_base}/entity/store/{opts.store_id}")
            response = await send_request(
                self._http,
                "GET",
                f"{opts.api_base.rstrip('/')}/entity/assortment",
                "moysklad",
                token,
                params={"filter": ";".join(filters), "limit": 1000},
                headers=headers,
            )
            raise_for_status(response, "moysklad", token)
            for row in response.json().get("rows", []):
                sku = str(row.get(opts.match_field) or "")
                if sku not in chunk:
                    continue
                stock = float(row.get("stock") or 0)
                reserve = float(row.get("reserve") or 0)
                quantity = row.get("quantity")
                available = float(quantity) if quantity is not None else stock - reserve
                previous = levels.get(sku)
                total = int(max(0.0, available)) + (previous.available if previous else 0)
                levels[sku] = StockLevel(sku=sku, found=True, available=total)
        return levels
