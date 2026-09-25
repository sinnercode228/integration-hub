"""Shared fixtures: an in-memory container with a controllable clock and mocked HTTP."""

from __future__ import annotations

import random
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest
import respx

from relay.config import RelayConfig, parse_config
from relay.container import Container, build_container
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
