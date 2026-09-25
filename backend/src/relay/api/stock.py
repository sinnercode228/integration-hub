"""Public stock endpoint for storefront widgets (MoySklad proxy)."""

from __future__ import annotations

import re
from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, Request, Response, status

from relay.api.deps import ContainerDep
from relay.api.schemas import StockItem, StockResponse
from relay.errors import UpstreamUnavailableError

router = APIRouter(tags=["stock"])

SKU_PATTERN = re.compile(r"^[\w\-./]{1,64}$")


def _cors(request: Request, response: Response, allowed: list[str]) -> None:
    origin = request.headers.get("origin")
    response.headers["Vary"] = "Origin"
    if "*" in allowed:
        response.headers["Access-Control-Allow-Origin"] = "*"
    elif origin and origin in allowed:
        response.headers["Access-Control-Allow-Origin"] = origin


@router.get("/api/stock", summary="Stock levels by SKU (token stays on the server)")
async def get_stock(
    request: Request,
    response: Response,
    container: ContainerDep,
    sku: Annotated[list[str] | None, Query(description="Repeatable: ?sku=A&sku=B")] = None,
    skus: Annotated[str | None, Query(description="Comma separated: ?skus=A,B")] = None,
) -> StockResponse:
    service = container.stock
    if service is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "stock proxy is not configured")
    requested = [*(sku or []), *((skus or "").split(","))]
    requested = [s.strip() for s in requested if s.strip()]
    if not requested:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, "pass at least one sku")
    invalid = [s for s in requested if not SKU_PATTERN.match(s)]
    if invalid:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, f"invalid sku: {invalid[0]!r}")

    _cors(request, response, container.settings.stock_cors_origins)
    try:
        lookup = await service.lookup(requested)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from None
    except UpstreamUnavailableError:
        container.metrics.stock_lookups.labels("error").inc()
        raise HTTPException(
            status.HTTP_503_SERVICE_UNAVAILABLE, "stock temporarily unavailable"
        ) from None

    metrics = container.metrics.stock_lookups
    metrics.labels("hit").inc(lookup.hits)
    metrics.labels("miss").inc(lookup.misses)
    if lookup.stale:
        metrics.labels("stale").inc()
    ttl = int(service.options.cache_ttl_seconds)
    response.headers["Cache-Control"] = f"public, max-age={0 if lookup.stale else ttl}"
    threshold = service.low_stock_threshold
    return StockResponse(
        items=[
            StockItem(sku=item.sku, available=item.available, status=item.status(threshold))
            for item in lookup.items
        ],
        stale=lookup.stale,
    )
