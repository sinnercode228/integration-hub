"""Route matching: decide which destinations an event goes to."""

from __future__ import annotations

import re
from collections.abc import Mapping
from fnmatch import fnmatchcase
from typing import Any

from relay.config import Condition, RelayConfig, RouteConfig, RouteTarget
from relay.templating import resolve_path


def _as_number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def condition_holds(condition: Condition, context: Mapping[str, Any]) -> bool:
    actual = resolve_path(condition.path, context)
    expected = condition.value
    match condition.op:
        case "exists":
            present = actual not in (None, "", [], {})
            return present if expected is None else present is bool(expected)
        case "eq":
            return actual == expected or (actual is not None and str(actual) == str(expected))
        case "ne":
            return not (actual == expected or (actual is not None and str(actual) == str(expected)))
        case "in" | "not_in":
            options = expected if isinstance(expected, list) else [expected]
            found = actual in options or str(actual) in {str(o) for o in options}
            return found if condition.op == "in" else not found
        case "contains":
            if actual is None:
                return False
            if isinstance(actual, (list, tuple, set, dict)):
                return expected in actual
            return str(expected).lower() in str(actual).lower()
        case "regex":
            return actual is not None and re.search(str(expected), str(actual)) is not None
        case "gt" | "lt":
            left, right = _as_number(actual), _as_number(expected)
            if left is None or right is None:
                return False
            return left > right if condition.op == "gt" else left < right
    return False  # pragma: no cover - Literal exhausts the options


def route_matches(route: RouteConfig, source: str, context: Mapping[str, Any]) -> bool:
    if not route.enabled:
        return False
    match = route.match
    if match.source is not None and source not in match.source:
        return False
    event_type = str(context.get("type", ""))
    if match.type is not None and not any(fnmatchcase(event_type, p) for p in match.type):
        return False
    return all(condition_holds(cond, context) for cond in match.where)


def plan(
    config: RelayConfig, source: str, context: Mapping[str, Any]
) -> list[tuple[RouteConfig, RouteTarget]]:
    """All (route, target) pairs that should receive the event, skipping disabled destinations."""
    targets: list[tuple[RouteConfig, RouteTarget]] = []
    for route in config.routes:
        if not route_matches(route, source, context):
            continue
        for target in route.deliver:
            destination = config.destinations.get(target.to)
            if destination is not None and destination.enabled:
                targets.append((route, target))
    return targets
