"""Process-level settings, read from ``RELAY_*`` environment variables (or ``.env``)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Literal

from pydantic import Field, SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="RELAY_", env_file=".env", env_file_encoding="utf-8", extra="ignore"
    )

    env: Literal["dev", "test", "prod"] = "dev"
    config_path: Path = Path("config/relay.yaml")

    # Infrastructure
    queue_backend: Literal["memory", "redis"] = "memory"
    store_backend: Literal["memory", "sqlite"] = "sqlite"
    redis_url: str = "redis://localhost:6379/0"
    redis_prefix: str = "relay"
    sqlite_path: Path = Path("data/relay.db")

    # Admin API / dashboard
    admin_token: SecretStr | None = None
    dashboard_dir: Path | None = None

    # Worker and retry policy
    run_worker: bool = True
    worker_concurrency: int = Field(default=4, ge=1, le=64)
    worker_poll_interval: float = Field(default=0.5, gt=0)
    lease_seconds: float = Field(default=120.0, gt=0)
    max_attempts: int = Field(default=8, ge=1, le=50)
    backoff_base_seconds: float = Field(default=2.0, gt=0)
    backoff_factor: float = Field(default=2.0, ge=1)
    backoff_max_seconds: float = Field(default=3600.0, gt=0)
    backoff_jitter: float = Field(default=0.2, ge=0, le=1)

    # HTTP edge
    max_body_bytes: int = Field(default=1_000_000, gt=0)
    http_timeout_seconds: float = Field(default=10.0, gt=0)
    idempotency_ttl_seconds: int = Field(default=7 * 24 * 3600, gt=0)
    stock_cors_origins: Annotated[list[str], NoDecode] = Field(default_factory=list)

    # Logging
    log_level: str = "INFO"
    log_json: bool = True

    @field_validator("stock_cors_origins", mode="before")
    @classmethod
    def _split_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [item.strip() for item in value.split(",") if item.strip()]
        return value

    @property
    def admin_auth_required(self) -> bool:
        return self.admin_token is not None or self.env == "prod"
