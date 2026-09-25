"""FastAPI application factory."""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.responses import RedirectResponse
from fastapi.staticfiles import StaticFiles

from relay import __version__
from relay.api import admin, ops, stock, webhooks
from relay.container import Container, build_container
from relay.log import configure_logging, get_logger
from relay.settings import Settings

log = get_logger(__name__)

ContainerFactory = Callable[[Settings], Awaitable[Container]]


def create_app(
    settings: Settings | None = None,
    *,
    container: Container | None = None,
    container_factory: ContainerFactory | None = None,
) -> FastAPI:
    settings = settings or (container.settings if container else Settings())

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        owned = container is None
        active = container or await (container_factory or build_container)(settings)
        app.state.container = active
        stop = asyncio.Event()
        worker_task: asyncio.Task[None] | None = None
        if settings.run_worker:
            worker_task = asyncio.create_task(
                active.worker.run(stop, settings.worker_poll_interval), name="relay-worker"
            )
        if settings.admin_token is None and settings.env != "prod":
            log.warning("admin.unprotected", hint="set RELAY_ADMIN_TOKEN outside local dev")
        log.info(
            "app.started",
            version=__version__,
            env=settings.env,
            queue=settings.queue_backend,
            store=settings.store_backend,
        )
        try:
            yield
        finally:
            stop.set()
            if worker_task is not None:
                await worker_task
            if owned:
                await active.aclose()

    app = FastAPI(
        title="Relay - integration hub",
        version=__version__,
        description="Webhooks in -> normalize -> deliver to many destinations, reliably.",
        lifespan=lifespan,
    )
    if container is not None:
        # Make the container available even without lifespan (httpx ASGITransport in tests).
        app.state.container = container

    @app.middleware("http")
    async def observe(
        request: Request, call_next: Callable[[Request], Awaitable[Response]]
    ) -> Response:
        request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:16]
        structlog.contextvars.clear_contextvars()
        structlog.contextvars.bind_contextvars(request_id=request_id)
        started = time.perf_counter()
        response = await call_next(request)
        elapsed = time.perf_counter() - started
        route = request.scope.get("route")
        route_path = getattr(route, "path", None) or "unmatched"
        active: Container | None = getattr(request.app.state, "container", None)
        if active is not None:
            active.metrics.http_requests.labels(
                request.method, route_path, str(response.status_code)
            ).inc()
            active.metrics.http_duration.labels(request.method, route_path).observe(elapsed)
        response.headers["X-Request-ID"] = request_id
        if route_path != "/metrics" and not route_path.startswith("/admin/assets"):
            log.info(
                "http.request",
                method=request.method,
                path=route_path,
                status=response.status_code,
                duration_ms=round(elapsed * 1000, 2),
            )
        return response

    app.include_router(ops.router)
    app.include_router(webhooks.router)
    app.include_router(stock.router)
    app.include_router(admin.router)

    dashboard = settings.dashboard_dir
    if dashboard is not None and Path(dashboard, "index.html").is_file():
        app.mount("/admin", StaticFiles(directory=dashboard, html=True), name="dashboard")

        @app.get("/", include_in_schema=False)
        async def root() -> RedirectResponse:
            return RedirectResponse("/admin/")

    return app


def app_factory() -> FastAPI:
    """Entry point for ``uvicorn relay.api.app:app_factory --factory``."""
    settings = Settings()
    configure_logging(settings.log_level, settings.log_json)
    return create_app(settings)
