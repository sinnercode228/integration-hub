"""Pure functions: signatures, forms, templating, config, routing, idempotency keys."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from relay.config import load_config, parse_config
from relay.domain import (
    Contact,
    DeliveryRecord,
    DeliveryStatus,
    EventRecord,
    EventStatus,
    NormalizedEvent,
    aggregate_status,
    new_id,
)
from relay.errors import ConfigError, SignatureError, TemplateError
from relay.forms import parse_form
from relay.idempotency import derive_key
from relay.routing import plan
from relay.security import redact, sign_payload, verify_signature
from relay.templating import normalize_phone, render, render_payload, resolve_path

from .conftest import REPO_ROOT, TEST_CONFIG

NOW = 1_760_000_000


class TestSignatures:
    def test_roundtrip(self) -> None:
        header = sign_payload("s3cret", b'{"a":1}', NOW)
        verify_signature("s3cret", b'{"a":1}', header, now=NOW + 10)

    @pytest.mark.parametrize(
        ("header", "message"),
        [
            (None, "missing"),
            ("garbage", "malformed"),
            ("t=abc,v1=00", "malformed"),
            (f"t={NOW - 1000},v1=00", "tolerance"),
            (f"t={NOW},v1=deadbeef", "mismatch"),
        ],
    )
    def test_rejects(self, header: str | None, message: str) -> None:
        with pytest.raises(SignatureError, match=message):
            verify_signature("s3cret", b"{}", header, now=NOW)

    def test_tampered_body_is_rejected(self) -> None:
        header = sign_payload("s3cret", b'{"amount":1}', NOW)
        with pytest.raises(SignatureError):
            verify_signature("s3cret", b'{"amount":1000}', header, now=NOW)

    def test_secret_rotation_accepts_any_v1(self) -> None:
        old = sign_payload("old", b"x", NOW).split(",")[1]
        new = sign_payload("new", b"x", NOW)
        verify_signature("new", b"x", f"{new},{old}", now=NOW)
        verify_signature("old", b"x", f"{new},{old}", now=NOW)

    def test_redact(self) -> None:
        assert redact("url /botABCD/send failed", "ABCD", None, "") == "url /bot***/send failed"
        assert redact("short ab", "ab") == "short ab"  # too short to be a real secret


class TestForms:
    def test_php_brackets(self) -> None:
        body = (
            "leads[status][0][id]=7&leads[status][0][status_id]=142"
            "&leads[status][1][id]=8&account[subdomain]=demo&tags[]=a&tags[]=b"
        )
        data = parse_form(body.encode())
        assert data["leads"]["status"][0] == {"id": "7", "status_id": "142"}
        assert data["leads"]["status"][1]["id"] == "8"
        assert data["account"] == {"subdomain": "demo"}
        assert data["tags"] == ["a", "b"]

    def test_plain_and_unicode(self) -> None:
        assert parse_form("Name=%D0%90%D0%BD%D0%BD%D0%B0&x=") == {"Name": "Анна", "x": ""}


class TestTemplating:
    ctx = {
        "id": "evt_1",
        "type": "form.submitted",
        "received_at": datetime(2026, 1, 2, 9, 30, tzinfo=UTC),
        "contact": {"name": "Анна", "phone": "8 (912) 345-67-89", "email": "A@Example.COM"},
        "fields": {"price": "1 990,50", "items": [{"sku": "A-1"}], "odd key": 1, "n": None},
    }

    @pytest.mark.parametrize(
        ("template", "expected"),
        [
            ("{{ contact.phone | phone }}", "+79123456789"),
            ("{{ contact.email | lower }}", "a@example.com"),
            ("{{ fields.price | int }}", 1990),
            ("{{ fields.price | float }}", 1990.5),
            ("{{ fields.items.0.sku }}", "A-1"),
            ("{{ fields['odd key'] }}", 1),
            ("{{ fields.missing | default('—') }}", "—"),
            ("{{ fields.n }}", None),
            ("{{ received_at | date('%d.%m.%Y %H:%M') }}", "02.01.2026 12:30"),
            ("{{ contact.name | upper }}!", "АННА!"),
            ("{{ 'x' | replace('x', 'y') }}", "y"),
            ("{{ 42 }}", 42),
            ("{{ fields.items | json }}", '[{"sku": "A-1"}]'),
        ],
    )
    def test_expressions(self, template: str, expected: object) -> None:
        assert render(template, self.ctx) == expected

    def test_nested_structures_keep_types(self) -> None:
        out = render({"a": ["{{ fields.price | int }}", "n={{ fields.price | int }}"]}, self.ctx)
        assert out == {"a": [1990, "n=1990"]}

    def test_truncate_and_join(self) -> None:
        ctx = {"t": "abcdefghij", "l": ["a", None, "b"]}
        assert render("{{ t | truncate(5) }}", ctx) == "abcd…"
        assert render("{{ l | join(' / ') }}", ctx) == "a / b"

    def test_escape_only_selected_fields(self) -> None:
        ctx = {"contact": {"name": "<script>alert(1)</script>"}}
        template = {"text": "<b>{{ contact.name }}</b>", "raw": "{{ contact.name }}"}
        out = render_payload(template, ctx, {"text": __import__("html").escape})
        assert out["text"] == "<b>&lt;script&gt;alert(1)&lt;/script&gt;</b>"
        assert out["raw"] == "<script>alert(1)</script>"

    @pytest.mark.parametrize(
        "template",
        [
            "{{ x | nope }}",
            "{{ x | default(__import__('os')) }}",
            "{{ x | 1bad }}",
            "{{ | lower }}",
        ],
    )
    def test_errors_and_no_code_execution(self, template: str) -> None:
        with pytest.raises(TemplateError):
            render(template, {"x": 1})

    def test_resolve_path_missing(self) -> None:
        assert resolve_path("a.b.c", {"a": {"b": 5}}) is None
        assert resolve_path("a.5", {"a": [1]}) is None

    @pytest.mark.parametrize(
        ("raw", "phone"),
        [
            ("89123456789", "+79123456789"),
            ("+7 912 345-67-89", "+79123456789"),
            ("9123456789", "+79123456789"),
            ("+44 20 7946 0958", "+442079460958"),
            ("123", "123"),
            (None, None),
        ],
    )
    def test_phone(self, raw: str | None, phone: str | None) -> None:
        assert normalize_phone(raw) == phone


class TestConfig:
    def test_repo_config_is_valid_with_defaults(self) -> None:
        config = load_config(REPO_ROOT / "config" / "relay.yaml", env={})
        assert {"tilda-site", "amocrm", "bitrix", "partner-api"} <= set(config.sources)
        assert len(config.routes) == 5
        summary = config.public_summary()
        assert "dev-tilda-key" not in str(summary)
        assert "dev-moysklad-token" not in str(summary)

    def test_env_interpolation(self) -> None:
        text = TEST_CONFIG.replace("tilda-key", "${TILDA_KEY}")
        config = parse_config(text, env={"TILDA_KEY": "from-env"})
        secret = config.sources["site"].secret
        assert secret is not None and secret.get_secret_value() == "from-env"

    def test_missing_env_is_reported(self) -> None:
        with pytest.raises(ConfigError, match="TILDA_KEY"):
            parse_config(TEST_CONFIG.replace("tilda-key", "${TILDA_KEY}"), env={})

    @pytest.mark.parametrize(
        ("patch", "message"),
        [
            (("- to: sheet", "- to: nowhere"), "unknown destination"),
            (("{source: partner}", "{source: ghost}"), "unknown source"),
            (("secret: partner-secret", "description: unsigned"), "secret"),
            (("name: won", "name: leads"), "duplicate route"),
            (("version: 1", "version: 1\nextra: true"), "invalid config"),
        ],
    )
    def test_validation(self, patch: tuple[str, str], message: str) -> None:
        with pytest.raises(ConfigError, match=message):
            parse_config(TEST_CONFIG.replace(*patch, 1), env={})

    def test_bad_yaml_and_missing_file(self, tmp_path: Path) -> None:
        with pytest.raises(ConfigError, match="YAML"):
            parse_config("a: [1", env={})
        with pytest.raises(ConfigError, match="mapping"):
            parse_config("- 1", env={})
        with pytest.raises(ConfigError, match="cannot read"):
            load_config(tmp_path / "missing.yaml")


class TestRouting:
    def _ctx(self, **extra: object) -> dict[str, object]:
        return {"type": "lead.status_changed", "fields": {"status_id": "142"}, **extra}

    def test_where_conditions(self, config) -> None:  # type: ignore[no-untyped-def]
        assert [r.name for r, _ in plan(config, "crm", self._ctx())] == ["won"]
        other = {"type": "lead.status_changed", "fields": {"status_id": "143"}}
        assert plan(config, "crm", other) == []

    def test_wildcard_source_route(self, config) -> None:  # type: ignore[no-untyped-def]
        targets = plan(config, "partner", {"type": "anything"})
        assert [(r.name, t.to) for r, t in targets] == [("partner", "hook")]

    @pytest.mark.parametrize(
        ("op", "value", "actual", "expected"),
        [
            ("ne", 1, 2, True),
            ("in", [1, 2], "2", True),
            ("not_in", [1, 2], 3, True),
            ("contains", "ПРО", "тариф про", True),
            ("contains", "a", ["a", "b"], True),
            ("contains", "x", None, False),
            ("exists", None, "", False),
            ("exists", False, None, True),
            ("regex", r"^\+7", "+79001234567", True),
            ("gt", 1000, "1500", True),
            ("lt", 10, "abc", False),
        ],
    )
    def test_operators(self, op: str, value: object, actual: object, expected: bool) -> None:
        from relay.config import Condition
        from relay.routing import condition_holds

        cond = Condition(path="v", op=op, value=value)  # type: ignore[arg-type]
        assert condition_holds(cond, {"v": actual}) is expected

    def test_invalid_regex_rejected(self) -> None:
        from relay.config import Condition

        with pytest.raises(ValueError, match="invalid regex"):
            Condition(path="v", op="regex", value="(")


class TestDomain:
    def _delivery(self, status: DeliveryStatus) -> DeliveryRecord:
        return DeliveryRecord(
            id=new_id("dlv"),
            event_id="e",
            route="r",
            destination="d",
            connector="c",
            status=status,
        )

    @pytest.mark.parametrize(
        ("statuses", "expected"),
        [
            ([], EventStatus.NO_ROUTE),
            ([DeliveryStatus.DELIVERED, DeliveryStatus.RETRYING], EventStatus.PROCESSING),
            ([DeliveryStatus.DELIVERED, DeliveryStatus.DELIVERED], EventStatus.DELIVERED),
            ([DeliveryStatus.DELIVERED, DeliveryStatus.DEAD], EventStatus.PARTIAL),
            ([DeliveryStatus.DEAD], EventStatus.FAILED),
        ],
    )
    def test_aggregate(self, statuses: list[DeliveryStatus], expected: EventStatus) -> None:
        assert aggregate_status(self._delivery(s) for s in statuses) is expected

    def test_ids_sort_by_time(self) -> None:
        first, second = new_id("evt"), new_id("evt")
        assert first.startswith("evt_") and len(first) == 28
        assert first[:16] <= second[:16]

    def test_idempotency_key_priority(self) -> None:
        with_ext = NormalizedEvent(type="t", external_id="42")
        without = NormalizedEvent(type="t")
        kw = {"body": b"{}", "index": 0, "total": 1}
        assert derive_key("s", with_ext, header_key=" k1 ", **kw) == "s:hdr:k1"  # type: ignore[arg-type]
        assert derive_key("s", with_ext, header_key=None, **kw) == "s:ext:t:42"  # type: ignore[arg-type]
        assert derive_key("s", without, header_key=None, **kw).startswith("s:body:")  # type: ignore[arg-type]
        multi = derive_key("s", without, header_key="k", body=b"", index=1, total=2)
        assert multi == "s:hdr:k:1"

    def test_template_context(self) -> None:
        event = EventRecord(
            id="evt_1",
            source="site",
            connector="tilda",
            type="form.submitted",
            idempotency_key="k",
            contact=Contact(name="A"),
        )
        ctx = event.template_context()
        assert ctx["contact"]["name"] == "A"
        assert "deliveries" not in ctx["event"]
