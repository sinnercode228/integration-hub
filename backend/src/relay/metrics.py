"""Prometheus metrics. Each app instance owns its registry (keeps tests isolated)."""

from __future__ import annotations

import json
import sys
import threading
from socketserver import ThreadingMixIn
from typing import Any
from wsgiref.simple_server import WSGIRequestHandler, WSGIServer, make_server
from wsgiref.types import StartResponse, WSGIEnvironment

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    Info,
    generate_latest,
)

from relay import __version__


class RelayMetrics:
    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry or CollectorRegistry(auto_describe=True)
        r = self.registry
        self.build_info = Info("relay_build", "Relay build information", registry=r)
        self.build_info.info({"version": __version__})

        self.events_received = Counter(
            "relay_events_received_total",
            "Inbound webhook events by source and ingestion result",
            ["source", "result"],
            registry=r,
        )
        self.deliveries = Counter(
            "relay_delivery_attempts_total",
            "Outbound delivery attempts by destination and outcome",
            ["destination", "connector", "outcome"],
            registry=r,
        )
        self.delivery_duration = Histogram(
            "relay_delivery_duration_seconds",
            "Duration of a single outbound delivery attempt",
            ["connector"],
            buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30),
            registry=r,
        )
        self.queue_depth = Gauge(
            "relay_queue_jobs",
            "Delivery jobs in the queue by state",
            ["state"],
            registry=r,
        )
        self.stock_lookups = Counter(
            "relay_stock_lookups_total",
            "Stock proxy SKU lookups by cache result",
            ["result"],
            registry=r,
        )
        self.http_requests = Counter(
            "relay_http_requests_total",
            "HTTP requests handled by the API",
            ["method", "route", "status"],
            registry=r,
        )
        self.http_duration = Histogram(
            "relay_http_request_duration_seconds",
            "HTTP request latency",
            ["method", "route"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5),
            registry=r,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)


class _ThreadingWSGIServer(ThreadingMixIn, WSGIServer):
    daemon_threads = True

    def handle_error(self, request: Any, client_address: Any) -> None:
        # The default prints a traceback to stderr, between the JSON log lines, also for a
        # client that hung up or timed out before its request was read. wsgiref already
        # ignores one that hangs up while the response is written.
        if not isinstance(sys.exception(), ConnectionError | TimeoutError):
            super().handle_error(request, client_address)  # pragma: no cover - a bug


class _QuietHandler(WSGIRequestHandler):
    timeout = 10  # seconds per read or write; a scraper sends its request as soon as it connects

    def log_message(self, format: str, *args: Any) -> None:
        """No access log: it would put plain lines between the JSON log lines on stderr."""


class MetricsServer:
    """``/metrics`` and ``/healthz`` for ``relay worker``, which runs without the API.

    Requests are answered from a separate thread; prometheus_client metrics can be read there
    while the worker's event loop updates them.
    """

    def __init__(self, metrics: RelayMetrics, host: str, port: int) -> None:
        self._metrics = metrics
        self._server = make_server(host, port, self._app, _ThreadingWSGIServer, _QuietHandler)
        self._thread = threading.Thread(
            target=self._server.serve_forever, name="relay-metrics", daemon=True
        )
        self._thread.start()

    def close(self) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join()

    def _app(self, environ: WSGIEnvironment, start_response: StartResponse) -> list[bytes]:
        path = environ.get("PATH_INFO")
        status = "200 OK"
        if path == "/metrics":
            content_type, body = CONTENT_TYPE_LATEST, self._metrics.render()
        elif path == "/healthz":
            content_type = "application/json"
            body = json.dumps({"status": "ok", "version": __version__}).encode()
        else:
            status, content_type, body = "404 Not Found", "text/plain", b"not found\n"
        start_response(status, [("Content-Type", content_type), ("Content-Length", str(len(body)))])
        return [body]
