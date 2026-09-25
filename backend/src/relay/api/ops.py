"""Operational endpoints: liveness, readiness, Prometheus metrics."""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST

from relay import __version__
from relay.api.deps import ContainerDep

router = APIRouter(tags=["ops"])


@router.get("/healthz", summary="Liveness probe")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "version": __version__}


@router.get("/readyz", summary="Readiness probe (store + queue reachable)")
async def readyz(container: ContainerDep) -> JSONResponse:
    checks = {"store": await container.store.ping(), "queue": await container.queue.ping()}
    ready = all(checks.values())
    return JSONResponse(
        {"status": "ready" if ready else "degraded", "checks": checks},
        status_code=200 if ready else 503,
    )


@router.get("/metrics", summary="Prometheus metrics", include_in_schema=False)
async def metrics(container: ContainerDep) -> Response:
    depth = await container.queue.depth()
    gauge = container.metrics.queue_depth
    gauge.labels("ready").set(depth.ready)
    gauge.labels("scheduled").set(depth.scheduled)
    gauge.labels("in_flight").set(depth.in_flight)
    gauge.labels("dead").set(depth.dead)
    return Response(container.metrics.render(), media_type=CONTENT_TYPE_LATEST)
