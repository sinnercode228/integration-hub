"""A tiny, safe template language for mapping rules.

Syntax: ``{{ path | filter | filter(arg, ...) }}``.

* ``path`` is a dotted lookup into the event context: ``contact.phone``, ``fields.items.0.sku``,
  ``raw['Your comment']``. A quoted string or a number is a literal.
* Filter arguments are Python literals parsed with :func:`ast.literal_eval` - nothing is
  ever executed, so templates coming from YAML cannot run code.
* A string that consists of exactly one expression keeps the value's type
  (``"{{ fields.price | int }}"`` renders to ``int``), otherwise values are interpolated as text.
"""

from __future__ import annotations

import ast
import html
import json
import re
from collections.abc import Callable, Mapping
from datetime import UTC, datetime
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from relay.errors import TemplateError

Escape = Callable[[str], str]

_EXPR = re.compile(r"\{\{\s*(.+?)\s*\}\}", re.DOTALL)
_PATH_TOKEN = re.compile(r"""\[\s*(['"])(.*?)\1\s*\]|\[\s*(\d+)\s*\]|\.?([^.\[\]]+)""")
_FILTER = re.compile(r"^([a-z_][a-z0-9_]*)\s*(?:\((.*)\))?$", re.DOTALL)
_MISSING = object()


def normalize_phone(value: Any) -> str | None:
    """Normalize a phone to E.164. Russian local formats (8xxx..., 9xx...) become +7..."""
    if value is None:
        return None
    text = str(value).strip()
    digits = re.sub(r"\D", "", text)
    if not digits:
        return text or None
    if len(digits) == 11 and digits[0] in "78":
        return "+7" + digits[1:]
    if len(digits) == 10 and digits[0] == "9":
        return "+7" + digits
    if 10 <= len(digits) <= 15:
        return "+" + digits
    return text


def _to_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=UTC)
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, UTC)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)
    return None


def _f_date(value: Any, fmt: str = "%d.%m.%Y %H:%M", tz: str = "Europe/Moscow") -> Any:
    moment = _to_datetime(value)
    if moment is None:
        return value
    try:
        zone: Any = ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        zone = UTC
    return moment.astimezone(zone).strftime(fmt)


def _f_default(value: Any, fallback: Any = "") -> Any:
    return fallback if value is None or value == "" or value == [] else value


def _f_int(value: Any, fallback: int = 0) -> int:
    try:
        return int(float(str(value).replace(" ", "").replace(",", ".")))
    except (TypeError, ValueError):
        return fallback


def _f_float(value: Any, fallback: float = 0.0) -> float:
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return fallback


def _f_truncate(value: Any, length: int = 100, suffix: str = "…") -> Any:
    if value is None:
        return value
    text = str(value)
    return text if len(text) <= length else text[: max(0, length - len(suffix))] + suffix


def _f_join(value: Any, sep: str = ", ") -> Any:
    if isinstance(value, (list, tuple)):
        return sep.join(str(item) for item in value if item not in (None, ""))
    return value


def _str_filter(fn: Callable[[str], str]) -> Callable[[Any], Any]:
    def wrapper(value: Any) -> Any:
        return None if value is None else fn(str(value))

    return wrapper


FILTERS: dict[str, Callable[..., Any]] = {
    "default": _f_default,
    "phone": normalize_phone,
    "lower": _str_filter(str.lower),
    "upper": _str_filter(str.upper),
    "strip": _str_filter(str.strip),
    "title": _str_filter(str.title),
    "escape": _str_filter(html.escape),
    "date": _f_date,
    "int": _f_int,
    "float": _f_float,
    "truncate": _f_truncate,
    "join": _f_join,
    "json": lambda value: json.dumps(value, ensure_ascii=False, default=str),
    "replace": lambda value, old, new: None if value is None else str(value).replace(old, new),
}


def _split_pipes(expr: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    quote: str | None = None
    depth = 0
    for char in expr:
        if quote:
            buf.append(char)
            if char == quote:
                quote = None
            continue
        if char in "'\"":
            quote = char
        elif char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
        elif char == "|" and depth == 0:
            parts.append("".join(buf).strip())
            buf = []
            continue
        buf.append(char)
    parts.append("".join(buf).strip())
    return parts


def resolve_path(path: str, context: Mapping[str, Any]) -> Any:
    """Resolve ``a.b.0['c d']`` against nested dicts/lists. Missing keys yield ``None``."""
    current: Any = context
    position = 0
    path = path.strip()
    while position < len(path):
        match = _PATH_TOKEN.match(path, position)
        if match is None or match.end() == position:
            raise TemplateError(f"invalid path {path!r}")
        position = match.end()
        key: str | int
        if match.group(2) is not None:
            key = match.group(2)
        elif match.group(3) is not None:
            key = int(match.group(3))
        else:
            token = match.group(4).strip()
            key = int(token) if token.isdigit() else token
        current = _step(current, key)
        if current is _MISSING:
            return None
    return current


def _step(current: Any, key: str | int) -> Any:
    if isinstance(current, Mapping):
        if key in current:
            return current[key]
        return current.get(str(key), _MISSING)
    if isinstance(current, (list, tuple)) and isinstance(key, int):
        return current[key] if -len(current) <= key < len(current) else _MISSING
    return _MISSING


def _literal(text: str) -> Any:
    if text[:1] in "'\"" or re.fullmatch(r"-?\d+(\.\d+)?", text) or text in ("True", "False"):
        try:
            return ast.literal_eval(text)
        except (ValueError, SyntaxError) as exc:
            raise TemplateError(f"invalid literal {text!r}") from exc
    if text in ("none", "null", "None"):
        return None
    return _MISSING


def evaluate(expr: str, context: Mapping[str, Any]) -> Any:
    head, *filters = _split_pipes(expr)
    if not head:
        raise TemplateError(f"empty expression in {{{{ {expr} }}}}")
    value = _literal(head)
    if value is _MISSING:
        value = resolve_path(head, context)
    for spec in filters:
        match = _FILTER.match(spec)
        if not match:
            raise TemplateError(f"invalid filter syntax {spec!r}")
        name, raw_args = match.group(1), match.group(2)
        func = FILTERS.get(name)
        if func is None:
            raise TemplateError(f"unknown filter {name!r}")
        args: tuple[Any, ...] = ()
        if raw_args and raw_args.strip():
            try:
                parsed = ast.literal_eval(f"({raw_args},)")
            except (ValueError, SyntaxError) as exc:
                raise TemplateError(f"filter {name!r}: arguments must be literals") from exc
            args = tuple(parsed)
        try:
            value = func(value, *args)
        except TypeError as exc:
            raise TemplateError(f"filter {name!r}: {exc}") from exc
    return value


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False, default=str)
    return str(value)


def render_string(template: str, context: Mapping[str, Any], escape: Escape | None = None) -> Any:
    single = _EXPR.fullmatch(template.strip())
    if single and "{{" not in single.group(1) and "}}" not in single.group(1):
        value = evaluate(single.group(1), context)
        if escape and isinstance(value, str):
            return escape(value)
        if isinstance(value, datetime):
            return value.isoformat()
        return value

    def substitute(match: re.Match[str]) -> str:
        text = _stringify(evaluate(match.group(1), context))
        return escape(text) if escape else text

    return _EXPR.sub(substitute, template)


def render(template: Any, context: Mapping[str, Any], escape: Escape | None = None) -> Any:
    """Render a nested template (dict / list / str / scalar)."""
    if isinstance(template, str):
        return render_string(template, context, escape)
    if isinstance(template, Mapping):
        return {str(key): render(value, context, escape) for key, value in template.items()}
    if isinstance(template, list):
        return [render(item, context, escape) for item in template]
    return template


def render_payload(
    template: Any,
    context: Mapping[str, Any],
    escape_fields: Mapping[str, Escape] | None = None,
) -> Any:
    """Render a destination payload, escaping user data only in the fields that need it
    (e.g. Telegram ``text`` with ``parse_mode=HTML`` or an e-mail ``html`` body)."""
    if isinstance(template, Mapping) and escape_fields:
        return {
            str(key): render(value, context, escape_fields.get(str(key)))
            for key, value in template.items()
        }
    return render(template, context)
