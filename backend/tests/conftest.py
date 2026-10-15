"""Shared fixtures: an in-memory container with a controllable clock and mocked HTTP."""

from __future__ import annotations

import asyncio
import os
import random
import uuid
from collections.abc import AsyncIterator, Callable
from pathlib import Path
from typing import Any

import fakeredis
import httpx
import pytest
import redis.exceptions
import respx
from redis.asyncio import Redis

from relay.config import RelayConfig, parse_config
from relay.container import Container, build_container
from relay.queue import RedisQueue
from relay.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[2]

TEST_CONFIG = """
version: 1
sources:
  site:
    connector: tilda
    secret: tilda-key
  crm:
    connector: amocrm
    secret: amo-token
  b24:
    connector: bitrix24
    secret: b24-token
  partner:
    connector: generic
    secret: partner-secret
destinations:
  tg:
    connector: telegram
    options:
      bot_token: "123456:SECRET-BOT-TOKEN"
      chat_id: "-100500"
      api_base: https://tg.test
  hook:
    connector: webhook
    max_attempts: 3
    options:
      url: https://hook.test/in
      secret: hook-secret
  sheet:
    connector: google_sheets
    options:
      spreadsheet_id: sheet-1
      range: "Leads!A:C"
      access_token: sheets-token
      api_base: https://sheets.test
routes:
  - name: leads
    match: {source: site, type: form.submitted}
    deliver:
      - to: tg
        template:
          text: "<b>Lead</b> {{ contact.name }} {{ contact.phone | phone }}"
      - to: sheet
        template:
          values: ["{{ contact.name }}", "{{ contact.phone | phone }}", "{{ id }}"]
  - name: orders
    match: {source: site, type: order.created}
    deliver:
      - to: hook
        template:
          order_id: "{{ fields.order.id }}"
          amount: "{{ fields.order.amount | float }}"
  - name: won
    match:
      source: crm
      type: lead.status_changed
      where:
        - {path: fields.status_id, op: eq, value: 142}
    deliver:
      - to: hook
  - name: partner
    match: {source: partner}
    deliver:
      - to: hook
stock:
  options:
    token: ms-secret-token
    api_base: https://ms.test/api
    cache_ttl_seconds: 60
    stale_ttl_seconds: 600
"""


class FakeClock:
    def __init__(self, start: float = 1_760_000_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


Workers = Callable[[int], tuple[list[Any], list[RedisQueue]]]


@pytest.fixture
async def workers(clock: FakeClock) -> AsyncIterator[Workers]:
    """Makes ``n`` queues on separate connections to one Redis server.

    The server is fakeredis, or a real Redis when ``RELAY_TEST_REDIS_URL`` is set (CI does).
    """
    url = os.environ.get("RELAY_TEST_REDIS_URL")
    server = fakeredis.FakeServer()
    prefix = f"race-{uuid.uuid4().hex}"
    clients: list[Any] = []

    def make(n: int) -> tuple[list[Any], list[RedisQueue]]:
        new = [
            Redis.from_url(url) if url else fakeredis.FakeAsyncRedis(server=server)
            for _ in range(n)
        ]
        clients.extend(new)
        return new, [RedisQueue(c, prefix=prefix, clock=clock) for c in new]

    yield make
    if clients:
        await clients[0].delete(*(f"{prefix}:q:{k}" for k in ("scheduled", "inflight", "dead")))
    for client in clients:
        await client.aclose()


async def no_answer(request: httpx.Request) -> httpx.Response:
    """A respx side effect: the server takes the request and never answers."""
    await asyncio.Event().wait()
    raise AssertionError("unreachable")


class LoseNextReply:
    """Lets Redis run the client's next script, then raises as if the reply was lost."""

    def __init__(self, client: Any) -> None:
        self._execute = client.execute_command
        self.lost = asyncio.Event()
        client.execute_command = self._wrapped

    async def _wrapped(self, *args: Any, **kwargs: Any) -> Any:
        result = await self._execute(*args, **kwargs)
        if args[0] == "EVALSHA" and not self.lost.is_set():
            self.lost.set()
            raise redis.exceptions.ConnectionError("Connection closed by server.")
        return result


@pytest.fixture
def config() -> RelayConfig:
    return parse_config(TEST_CONFIG, env={})


@pytest.fixture
def settings(tmp_path: Path) -> Settings:
    return Settings(
        _env_file=None,  # type: ignore[call-arg]
        env="test",
        store_backend="memory",
        queue_backend="memory",
        run_worker=False,
        backoff_jitter=0,
        backoff_base_seconds=2,
        max_attempts=4,
        sqlite_path=tmp_path / "relay.db",
        log_json=False,
    )


@pytest.fixture
def mock_http() -> respx.MockRouter:
    with respx.mock(assert_all_called=False, assert_all_mocked=True) as router:
        yield router


@pytest.fixture
async def container(
    settings: Settings, config: RelayConfig, clock: FakeClock, mock_http: respx.MockRouter
) -> AsyncIterator[Container]:
    http = httpx.AsyncClient()
    built = await build_container(settings, config, http=http, clock=clock, rng=random.Random(1))
    try:
        yield built
    finally:
        await built.aclose()
        await http.aclose()


@pytest.fixture
async def api(container: Container) -> AsyncIterator[httpx.AsyncClient]:
    from relay.api.app import create_app

    app = create_app(container.settings, container=container)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://relay.test") as client:
        yield client
