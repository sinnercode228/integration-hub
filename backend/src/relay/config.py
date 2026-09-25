"""Declarative integration config (``config/relay.yaml``).

The YAML file describes *sources* (who sends webhooks to us), *destinations* (where we
deliver) and *routes* (which events go where, and how the payload is mapped). Secrets are
never written into the file: use ``${ENV_VAR}`` or ``${ENV_VAR:-default}`` placeholders.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError, model_validator

from relay.errors import ConfigError

_ENV_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-(.*?))?\}")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class SourceConfig(_Strict):
    connector: str
    secret: SecretStr | None = None
    allow_unsigned: bool = False
    enabled: bool = True
    description: str | None = None
    options: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _require_secret(self) -> SourceConfig:
        if self.secret is not None and not self.secret.get_secret_value():
            self.secret = None
        if self.secret is None and not self.allow_unsigned:
            raise ValueError(
                "a source needs a 'secret' (or explicitly 'allow_unsigned: true' for local dev)"
            )
        return self


class DestinationConfig(_Strict):
    connector: str
    enabled: bool = True
    description: str | None = None
    timeout_seconds: float | None = Field(default=None, gt=0)
    max_attempts: int | None = Field(default=None, ge=1, le=50)
    options: dict[str, Any] = Field(default_factory=dict)


ConditionOp = Literal["eq", "ne", "in", "not_in", "contains", "exists", "regex", "gt", "lt"]


class Condition(_Strict):
    path: str
    op: ConditionOp = "eq"
    value: Any = None

    @model_validator(mode="after")
    def _check_regex(self) -> Condition:
        if self.op == "regex":
            try:
                re.compile(str(self.value))
            except re.error as exc:
                raise ValueError(f"invalid regex {self.value!r}: {exc}") from exc
        return self


class RouteMatch(_Strict):
    source: list[str] | None = None
    type: list[str] | None = None
    where: list[Condition] = Field(default_factory=list)

    @model_validator(mode="before")
    @classmethod
    def _scalars_to_lists(cls, data: Any) -> Any:
        if isinstance(data, dict):
            data = dict(data)
            for key in ("source", "type"):
                if isinstance(data.get(key), str):
                    data[key] = [data[key]]
        return data


class RouteTarget(_Strict):
    to: str
    template: Any = None


class RouteConfig(_Strict):
    name: str = Field(pattern=r"^[a-z0-9][a-z0-9_.-]{0,63}$")
    description: str | None = None
    enabled: bool = True
    match: RouteMatch = Field(default_factory=RouteMatch)
    deliver: list[RouteTarget] = Field(min_length=1)


class StockConfig(_Strict):
    connector: Literal["moysklad"] = "moysklad"
    options: dict[str, Any] = Field(default_factory=dict)


class RelayConfig(_Strict):
    version: Literal[1] = 1
    sources: dict[str, SourceConfig] = Field(default_factory=dict)
    destinations: dict[str, DestinationConfig] = Field(default_factory=dict)
    routes: list[RouteConfig] = Field(default_factory=list)
    stock: StockConfig | None = None

    @model_validator(mode="after")
    def _check_references(self) -> RelayConfig:
        id_pattern = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
        for kind, ids in (("source", self.sources), ("destination", self.destinations)):
            for key in ids:
                if not id_pattern.match(key):
                    raise ValueError(f"{kind} id {key!r} must match {id_pattern.pattern}")
        seen: set[str] = set()
        for route in self.routes:
            if route.name in seen:
                raise ValueError(f"duplicate route name {route.name!r}")
            seen.add(route.name)
            for source in route.match.source or []:
                if source not in self.sources:
                    raise ValueError(f"route {route.name!r} references unknown source {source!r}")
            for target in route.deliver:
                if target.to not in self.destinations:
                    raise ValueError(
                        f"route {route.name!r} references unknown destination {target.to!r}"
                    )
        return self

    def find_target(self, route_name: str, destination: str) -> RouteTarget | None:
        for route in self.routes:
            if route.name == route_name:
                for target in route.deliver:
                    if target.to == destination:
                        return target
        return None

    def public_summary(self) -> dict[str, Any]:
        """Config without secrets or connector options - safe for the admin dashboard."""
        return {
            "sources": [
                {
                    "id": key,
                    "connector": src.connector,
                    "enabled": src.enabled,
                    "signed": src.secret is not None,
                    "description": src.description,
                }
                for key, src in self.sources.items()
            ],
            "destinations": [
                {
                    "id": key,
                    "connector": dst.connector,
                    "enabled": dst.enabled,
                    "description": dst.description,
                }
                for key, dst in self.destinations.items()
            ],
            "routes": [
                {
                    "name": route.name,
                    "description": route.description,
                    "enabled": route.enabled,
                    "match": route.match.model_dump(),
                    "deliver": [target.model_dump() for target in route.deliver],
                }
                for route in self.routes
            ],
            "stock": {"connector": self.stock.connector} if self.stock else None,
        }


def interpolate_env(value: Any, env: Mapping[str, str]) -> Any:
    """Replace ``${VAR}`` / ``${VAR:-default}`` in every string of a parsed YAML tree."""
    missing: list[str] = []

    def replace(match: re.Match[str]) -> str:
        name, default = match.group(1), match.group(2)
        if name in env:
            return env[name]
        if default is not None:
            return default
        missing.append(name)
        return ""

    def walk(node: Any) -> Any:
        if isinstance(node, str):
            return _ENV_PATTERN.sub(replace, node)
        if isinstance(node, list):
            return [walk(item) for item in node]
        if isinstance(node, dict):
            return {key: walk(item) for key, item in node.items()}
        return node

    result = walk(value)
    if missing:
        names = ", ".join(sorted(set(missing)))
        raise ConfigError(f"environment variables are not set: {names}")
    return result


def parse_config(text: str, env: Mapping[str, str] | None = None) -> RelayConfig:
    try:
        data = yaml.safe_load(text) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML: {exc}") from exc
    if not isinstance(data, dict):
        raise ConfigError("config root must be a mapping")
    data = interpolate_env(data, os.environ if env is None else env)
    try:
        return RelayConfig.model_validate(data)
    except ValidationError as exc:
        raise ConfigError(f"invalid config:\n{exc}") from exc


def load_config(path: Path, env: Mapping[str, str] | None = None) -> RelayConfig:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise ConfigError(f"cannot read config {path}: {exc}") from exc
    return parse_config(text, env)
