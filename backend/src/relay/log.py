"""Structured logging via structlog: JSON lines in production, readable console in dev.

Request ids and event/delivery ids are bound through ``contextvars`` so every log line
emitted while handling a webhook carries them automatically.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

_SENSITIVE_KEYS = {"token", "secret", "password", "authorization", "api_key", "access_token"}


def _drop_secrets(_: Any, __: str, event_dict: dict[str, Any]) -> dict[str, Any]:
    for key in list(event_dict):
        if key.lower() in _SENSITIVE_KEYS:
            event_dict[key] = "***"
    return event_dict


def configure_logging(level: str = "INFO", json_output: bool = True) -> None:
    log_level = logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
    shared: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        _drop_secrets,
    ]
    renderer: Any = (
        structlog.processors.JSONRenderer(ensure_ascii=False)
        if json_output
        else structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())
    )
    structlog.configure(
        processors=[*shared, structlog.processors.format_exc_info, renderer],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=False,
    )
    logging.basicConfig(level=log_level, stream=sys.stderr, format="%(message)s")
    for noisy in ("httpx", "httpcore", "uvicorn.access"):
        logging.getLogger(noisy).setLevel(logging.WARNING)


def get_logger(name: str) -> Any:
    return structlog.get_logger(name)
