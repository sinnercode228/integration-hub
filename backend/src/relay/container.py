"""Composition root: builds every component from ``Settings`` + ``RelayConfig``.

Tests use the same function with in-memory backends and a mocked HTTP transport, so the
code under test is exactly the code that runs in production.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import httpx
from redis.asyncio import Redis

from relay import __version__
from relay.config import RelayConfig, load_config
from relay.connectors import INBOUND, OUTBOUND
from relay.connectors.base import InboundConnector, OutboundConnector
from relay.connectors.moysklad import MoySkladStockService
from relay.errors import ConfigError
from relay.idempotency import IdempotencyStore, InMemoryIdempotencyStore, RedisIdempotencyStore
from relay.ingest import IngestService
from relay.metrics import RelayMetrics
from relay.queue import DeliveryQueue, InMemoryQueue, RedisQueue
from relay.settings import Settings
from relay.store import EventStore, InMemoryEventStore, SqliteEventStore
from relay.worker import DeliveryWorker, RetryPolicy


@dataclass
class Container:
    settings: Settings
    config: RelayConfig
    http: httpx.AsyncClient
    store: EventStore
    queue: DeliveryQueue
    idempotency: IdempotencyStore
    metrics: RelayMetrics
    inbound: dict[str, InboundConnector]
    destinations: dict[str, OutboundConnector]
    ingest: IngestService
    worker: DeliveryWorker
    stock: MoySkladStockService | None = None
    redis: Redis | None = None
    _owns_http: bool = field(default=True, repr=False)

    async def aclose(self) -> None:
        for connector in self.destinations.values():
            await connector.aclose()
        await self.store.close()
        await self.queue.close()
        if self._owns_http:
            await self.http.aclose()


DEV_SECRET_PREFIX = "dev-"  # noqa: S105 - marker of relay.yaml placeholders, not a secret


def _reject_dev_secrets(config: RelayConfig) -> None:
    """Refuse to run in prod with the ``${VAR:-dev-...}`` placeholders from relay.yaml."""
    weak = [
        source_id
        for source_id, source in config.sources.items()
        if source.secret is not None
        and source.secret.get_secret_value().startswith(DEV_SECRET_PREFIX)
    ]
    if weak:
        raise ConfigError(
            "RELAY_ENV=prod but these sources still use development placeholder secrets: "
            + ", ".join(sorted(weak))
        )


def build_inbound(config: RelayConfig, http: httpx.AsyncClient) -> dict[str, InboundConnector]:
    connectors: dict[str, InboundConnector] = {}
    for source_id, source in config.sources.items():
        cls = INBOUND.get(source.connector)
        if cls is None:
            known = ", ".join(sorted(INBOUND))
            raise ConfigError(
                f"source {source_id!r}: unknown connector {source.connector!r} ({known})"
            )
        connectors[source_id] = cls(
            source_id,
            secret=source.secret.get_secret_value() if source.secret else None,
            options=source.options,
            http=http,
        )
    return connectors


def build_destinations(
    config: RelayConfig,
    http: httpx.AsyncClient,
    overrides: dict[str, OutboundConnector] | None = None,
) -> dict[str, OutboundConnector]:
    connectors: dict[str, OutboundConnector] = dict(overrides or {})
    for name, destination in config.destinations.items():
        if name in connectors:
            continue
        cls = OUTBOUND.get(destination.connector)
        if cls is None:
            known = ", ".join(sorted(OUTBOUND))
            raise ConfigError(
                f"destination {name!r}: unknown connector {destination.connector!r} ({known})"
            )
        connectors[name] = cls(name, options=destination.options, http=http)
    return connectors


async def build_container(
    settings: Settings,
    config: RelayConfig | None = None,
    *,
    http: httpx.AsyncClient | None = None,
    redis_client: Redis | None = None,
    destination_overrides: dict[str, OutboundConnector] | None = None,
    clock: Callable[[], float] = time.time,
    rng: Any = None,
) -> Container:
    config = config or load_config(settings.config_path)
    if settings.env == "prod":
        _reject_dev_secrets(config)
    owns_http = http is None
    http = http or httpx.AsyncClient(
        timeout=httpx.Timeout(settings.http_timeout_seconds),
        headers={"User-Agent": f"Relay/{__version__}"},
        follow_redirects=False,
    )
    metrics = RelayMetrics()

    redis = redis_client
    if redis is None and settings.queue_backend == "redis":
        redis = Redis.from_url(settings.redis_url)

    queue: DeliveryQueue
    idempotency: IdempotencyStore
    if settings.queue_backend == "redis":
        assert redis is not None
        queue = RedisQueue(redis, prefix=settings.redis_prefix, clock=clock)
        idempotency = RedisIdempotencyStore(redis, prefix=settings.redis_prefix)
    else:
        queue = InMemoryQueue(clock=clock)
        idempotency = InMemoryIdempotencyStore(clock=clock)

    store: EventStore = (
        SqliteEventStore(settings.sqlite_path)
        if settings.store_backend == "sqlite"
        else InMemoryEventStore()
    )

    inbound = build_inbound(config, http)
    destinations = build_destinations(config, http, destination_overrides)
    policy = RetryPolicy(
        max_attempts=settings.max_attempts,
        base_delay=settings.backoff_base_seconds,
        factor=settings.backoff_factor,
        max_delay=settings.backoff_max_seconds,
        jitter=settings.backoff_jitter,
    )
    ingest = IngestService(
        config=config,
        inbound=inbound,
        store=store,
        queue=queue,
        idempotency=idempotency,
        metrics=metrics,
        idempotency_ttl=settings.idempotency_ttl_seconds,
        clock=clock,
    )
    worker = DeliveryWorker(
        config=config,
        store=store,
        queue=queue,
        destinations=destinations,
        metrics=metrics,
        policy=policy,
        concurrency=settings.worker_concurrency,
        lease_seconds=settings.lease_seconds,
        default_timeout=max(settings.http_timeout_seconds * 2, 5.0),
        clock=clock,
        rng=rng,
    )
    stock = MoySkladStockService(config.stock.options, http) if config.stock else None
    return Container(
        settings=settings,
        config=config,
        http=http,
        store=store,
        queue=queue,
        idempotency=idempotency,
        metrics=metrics,
        inbound=inbound,
        destinations=destinations,
        ingest=ingest,
        worker=worker,
        stock=stock,
        redis=redis,
        _owns_http=owns_http,
    )
