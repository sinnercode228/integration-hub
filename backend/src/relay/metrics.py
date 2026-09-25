"""Prometheus metrics. Each app instance owns its registry (keeps tests isolated)."""

from __future__ import annotations

from prometheus_client import (
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
