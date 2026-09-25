"""Command line: ``relay serve | worker | check-config | sign``."""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import signal
import sys
import time
from pathlib import Path

from relay import __version__
from relay.config import load_config
from relay.errors import ConfigError
from relay.log import configure_logging, get_logger
from relay.security import sign_payload
from relay.settings import Settings


def _serve(args: argparse.Namespace) -> int:
    import uvicorn

    uvicorn.run(
        "relay.api.app:app_factory",
        factory=True,
        host=args.host,
        port=args.port,
        proxy_headers=True,
        log_config=None,
    )
    return 0


async def _run_worker(settings: Settings) -> None:
    from relay.container import build_container

    log = get_logger("relay.worker")
    container = await build_container(settings)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # Windows
            loop.add_signal_handler(sig, stop.set)
    try:
        await container.worker.run(stop, settings.worker_poll_interval)
    finally:
        await container.aclose()
        log.info("worker.shutdown_complete")


def _worker(_: argparse.Namespace) -> int:
    settings = Settings()
    configure_logging(settings.log_level, settings.log_json)
    asyncio.run(_run_worker(settings))
    return 0


def _check_config(args: argparse.Namespace) -> int:
    path = Path(args.config) if args.config else Settings().config_path
    import httpx

    from relay.container import build_destinations, build_inbound

    async def validate_connectors() -> None:
        async with httpx.AsyncClient() as http:
            build_inbound(config, http)
            build_destinations(config, http)

    try:
        config = load_config(path)
        asyncio.run(validate_connectors())
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        return 1
    print(
        f"OK: {len(config.sources)} sources, {len(config.destinations)} destinations, "
        f"{len(config.routes)} routes, stock proxy: {'on' if config.stock else 'off'}"
    )
    return 0


def _sign(args: argparse.Namespace) -> int:
    body = Path(args.file).read_bytes() if args.file else sys.stdin.buffer.read()
    print(sign_payload(args.secret, body, int(args.timestamp or time.time())))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="relay", description="Relay integration hub")
    parser.add_argument("--version", action="version", version=f"relay {__version__}")
    sub = parser.add_subparsers(dest="command", required=True)

    serve = sub.add_parser("serve", help="run the HTTP API (and the in-process worker)")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8000)
    serve.set_defaults(func=_serve)

    worker = sub.add_parser("worker", help="run only the delivery worker")
    worker.set_defaults(func=_worker)

    check = sub.add_parser("check-config", help="validate the YAML config and connector options")
    check.add_argument("--config", help="path to relay.yaml (default: RELAY_CONFIG_PATH)")
    check.set_defaults(func=_check_config)

    sign = sub.add_parser("sign", help="compute X-Relay-Signature for a body (generic source)")
    sign.add_argument("--secret", required=True)
    sign.add_argument("--file", help="body file (default: stdin)")
    sign.add_argument("--timestamp", type=int)
    sign.set_defaults(func=_sign)

    args = parser.parse_args(argv)
    result: int = args.func(args)
    return result


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
