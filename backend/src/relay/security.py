"""Signing and verification primitives (HMAC-SHA256, constant-time comparisons)."""

from __future__ import annotations

import hashlib
import hmac

from relay.errors import SignatureError

SIGNATURE_HEADER = "x-relay-signature"
DEFAULT_TOLERANCE_SECONDS = 300


def secure_compare(provided: str | None, expected: str | None) -> bool:
    if not provided or not expected:
        return False
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


def hmac_sha256_hex(secret: str, message: bytes) -> str:
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def sign_payload(secret: str, body: bytes, timestamp: int) -> str:
    """Build a ``t=<unix>,v1=<hex>`` header value. The timestamp is part of the signed
    message, which makes captured requests useless after the tolerance window."""
    digest = hmac_sha256_hex(secret, f"{timestamp}.".encode() + body)
    return f"t={timestamp},v1={digest}"


def verify_signature(
    secret: str,
    body: bytes,
    header: str | None,
    *,
    now: float,
    tolerance: int = DEFAULT_TOLERANCE_SECONDS,
) -> None:
    if not header:
        raise SignatureError("missing signature header")
    parts: dict[str, list[str]] = {}
    for item in header.split(","):
        key, _, value = item.strip().partition("=")
        if key and value:
            parts.setdefault(key, []).append(value)
    try:
        timestamp = int(parts["t"][0])
    except (KeyError, ValueError) as exc:
        raise SignatureError("malformed signature header") from exc
    if abs(now - timestamp) > tolerance:
        raise SignatureError("signature timestamp is outside the tolerance window")
    expected = hmac_sha256_hex(secret, f"{timestamp}.".encode() + body)
    # Several v1 values are allowed so a sender can sign with old and new secret during rotation.
    if not any(secure_compare(candidate, expected) for candidate in parts.get("v1", [])):
        raise SignatureError("signature mismatch")


def redact(text: str, *secrets: str | None) -> str:
    """Remove secret values (tokens in URLs, passwords) from error messages and logs."""
    for secret in secrets:
        if secret and len(secret) >= 4:
            text = text.replace(secret, "***")
    return text
