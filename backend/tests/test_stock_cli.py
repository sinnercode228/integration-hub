"""MoySklad stock proxy (cache, single flight, stale-if-error), CLI, app lifespan."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx
import pytest
import respx

from relay.cli import main
from relay.connectors.moysklad import MoySkladStockService
from relay.container import Container
from relay.errors import UpstreamUnavailableError
from relay.security import verify_signature
from relay.settings import Settings

from .conftest import REPO_ROOT

MS_URL = "https://ms.test/api/entity/assortment"
OPTIONS = {
    "token": "ms-secret-token",
    "api_base": "https://ms.test/api",
    "cache_ttl_seconds": 60,
    "stale_ttl_seconds": 600,
    "low_stock_threshold": 3,
}
ROWS = {
    "rows": [
        {"article": "MUG-1", "stock": 10, "reserve": 2},
        {"article": "TEE-2", "quantity": 2},
        {"article": "OTHER", "stock": 99},
    ]
}


class TestStockService:
    async def test_cache_and_statuses(self) -> None:
        now = [0.0]
        async with httpx.AsyncClient() as http:
            service = MoySkladStockService(OPTIONS, http, clock=lambda: now[0])
            with respx.mock:
                route = respx.get(MS_URL).respond(json=ROWS)
                first = await service.lookup(["MUG-1", "TEE-2", "NONE-3", "MUG-1"])
                assert [(i.sku, i.available, i.status(3)) for i in first.items] == [
                    ("MUG-1", 8, "in_stock"),
                    ("TEE-2", 2, "low"),
                    ("NONE-3", 0, "out_of_stock"),
                ]
                assert (first.hits, first.misses, first.stale) == (0, 3, False)
                request = route.calls.last.request
                assert request.headers["authorization"] == "Bearer ms-secret-token"
                assert request.url.params["filter"] == "article=MUG-1;article=TEE-2;article=NONE-3"

                second = await service.lookup(["MUG-1"])
                assert (second.hits, second.misses) == (1, 0)
                assert route.call_count == 1
                now[0] = 61
                await service.lookup(["MUG-1"])
                assert route.call_count == 2

    async def test_single_flight(self) -> None:
        async with httpx.AsyncClient() as http:
            service = MoySkladStockService(OPTIONS, http)
            with respx.mock:
                route = respx.get(MS_URL).respond(json=ROWS)
                results = await asyncio.gather(*(service.lookup(["MUG-1"]) for _ in range(20)))
        assert route.call_count == 1
        assert all(r.items[0].available == 8 for r in results)

    async def test_stale_if_error(self) -> None:
        now = [0.0]
        async with httpx.AsyncClient() as http:
            service = MoySkladStockService(OPTIONS, http, clock=lambda: now[0])
            with respx.mock:
                route = respx.get(MS_URL).respond(json=ROWS)
                await service.lookup(["MUG-1"])
                route.respond(503)
                now[0] = 120
                stale = await service.lookup(["MUG-1"])
                assert stale.stale and stale.items[0].available == 8
                now[0] = 1000
                with pytest.raises(UpstreamUnavailableError):
                    await service.lookup(["MUG-1"])
                with pytest.raises(UpstreamUnavailableError):
                    await service.lookup(["NEW-1"])

    async def test_too_many_skus(self) -> None:
        async with httpx.AsyncClient() as http:
            service = MoySkladStockService({**OPTIONS, "max_skus": 2}, http)
            with pytest.raises(ValueError, match="too many"):
                await service.lookup(["a", "b", "c"])


class TestStockEndpoint:
    async def test_proxy_hides_token(
        self, api: httpx.AsyncClient, container: Container, mock_http: respx.MockRouter
    ) -> None:
        container.settings.stock_cors_origins = ["https://shop.test"]
        mock_http.get(MS_URL).respond(json=ROWS)
        response = await api.get(
            "/api/stock?sku=MUG-1&skus=TEE-2,NONE-3", headers={"Origin": "https://shop.test"}
        )
        assert response.status_code == 200
        assert response.json() == {
            "items": [
                {"sku": "MUG-1", "available": 8, "status": "in_stock"},
                {"sku": "TEE-2", "available": 2, "status": "low"},
                {"sku": "NONE-3", "available": 0, "status": "out_of_stock"},
            ],
            "stale": False,
        }
        assert "ms-secret-token" not in response.text
        assert response.headers["access-control-allow-origin"] == "https://shop.test"
        assert response.headers["cache-control"] == "public, max-age=60"
        other = await api.get("/api/stock?sku=MUG-1", headers={"Origin": "https://evil.test"})
        assert "access-control-allow-origin" not in other.headers
        metrics = (await api.get("/metrics")).text
        assert 'relay_stock_lookups_total{result="hit"} 1.0' in metrics

    async def test_validation_and_outage(
        self, api: httpx.AsyncClient, mock_http: respx.MockRouter
    ) -> None:
        assert (await api.get("/api/stock")).status_code == 422
        assert (await api.get("/api/stock?sku=bad%20sku")).status_code == 422
        mock_http.get(MS_URL).respond(502)
        assert (await api.get("/api/stock?sku=X-1")).status_code == 503


class TestCli:
    def test_check_config(self, capsys: pytest.CaptureFixture[str]) -> None:
        assert main(["check-config", "--config", str(REPO_ROOT / "config" / "relay.yaml")]) == 0
        assert "5 routes" in capsys.readouterr().out

    def test_check_config_error(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        bad = tmp_path / "bad.yaml"
        bad.write_text("sources: {x: {connector: nope, allow_unsigned: true}}")
        assert main(["check-config", "--config", str(bad)]) == 1
        assert "unknown connector" in capsys.readouterr().err

    def test_sign(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        body = tmp_path / "body.json"
        body.write_bytes(b'{"a":1}')
        assert main(["sign", "--secret", "s", "--file", str(body), "--timestamp", "100"]) == 0
        header = capsys.readouterr().out.strip()
        verify_signature("s", b'{"a":1}', header, now=100)


async def test_app_lifespan_runs_worker(tmp_path: Path) -> None:
    """Full app with sqlite store, a background worker and the dashboard mounted."""
    from relay.api.app import create_app

    dashboard = tmp_path / "dash"
    dashboard.mkdir()
    (dashboard / "index.html").write_text("<h1>dashboard</h1>")
    config = tmp_path / "relay.yaml"
    config.write_text(
        "sources: {p: {connector: generic, allow_unsigned: true}}\n"
        "destinations: {h: {connector: webhook, options: {url: 'https://h.test/'}}}\n"
        "routes: [{name: all, deliver: [{to: h}]}]\n"
    )
    settings = Settings(
        _env_file=None,  # type: ignore[call-arg]
        config_path=config,
        sqlite_path=tmp_path / "relay.db",
        dashboard_dir=dashboard,
        worker_poll_interval=0.01,
        log_json=False,
    )
    app = create_app(settings)
    with respx.mock(assert_all_mocked=True) as router:
        hook = router.post("https://h.test/").respond(200)
        async with app.router.lifespan_context(app):
            transport = httpx.ASGITransport(app=app)
            async with httpx.AsyncClient(transport=transport, base_url="http://t") as client:
                response = await client.post(
                    "/webhooks/p",
                    content=json.dumps({"type": "a", "id": 1}),
                    headers={"content-type": "application/json"},
                )
                assert response.status_code == 202
                for _ in range(200):
                    if hook.called:
                        break
                    await asyncio.sleep(0.01)
                assert hook.called
                assert (await client.get("/admin/")).text == "<h1>dashboard</h1>"
                assert (await client.get("/")).status_code == 307
