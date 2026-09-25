"""Idempotency keys: the same webhook delivered twice must create exactly one event.

CRMs and form builders retry webhooks on timeouts, so duplicates are normal traffic. A key
is *claimed* atomically (``SET NX`` in Redis); a second claim returns the id of the event
created by the first one, and the API answers ``200 duplicate`` instead of re-processing.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from collections.abc import Callable
from typing import Any, Protocol

from redis.asyncio import Redis

from relay.domain import NormalizedEvent


class IdempotencyStore(Protocol):
    async def claim(self, key: str, value: str, ttl_seconds: int) -> str | None:
        """Claim ``key``. Returns ``None`` if claimed now, otherwise the existing value."""
        ...

    async def release(self, key: str) -> None: ...


class InMemoryIdempotencyStore:
    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._items: dict[str, tuple[str, float]] = {}
        self._lock = asyncio.Lock()

    async def claim(self, key: str, value: str, ttl_seconds: int) -> str | None:
        async with self._lock:
            now = self._clock()
            existing = self._items.get(key)
            if existing and existing[1] > now:
                return existing[0]
            self._items[key] = (value, now + ttl_seconds)
            if len(self._items) > 10_000:
                self._items = {k: v for k, v in self._items.items() if v[1] > now}
            return None

    async def release(self, key: str) -> None:
        async with self._lock:
            self._items.pop(key, None)


class RedisIdempotencyStore:
    def __init__(self, client: Redis, prefix: str = "relay") -> None:
        self._redis = client
        self._prefix = f"{prefix}:idem:"

    async def claim(self, key: str, value: str, ttl_seconds: int) -> str | None:
        full_key = self._prefix + key
        created = await self._redis.set(full_key, value, nx=True, ex=ttl_seconds)
        if created:
            return None
        existing: Any = await self._redis.get(full_key)
        if existing is None:  # expired between SET and GET: claim again
            return await self.claim(key, value, ttl_seconds)
        return existing.decode() if isinstance(existing, bytes) else str(existing)

    async def release(self, key: str) -> None:
        await self._redis.delete(self._prefix + key)


def derive_key(
    source: str,
    event: NormalizedEvent,
    *,
    header_key: str | None,
    body: bytes,
    index: int,
    total: int,
) -> str:
    """Pick the strongest available idempotency key.

    1. ``Idempotency-Key`` header sent by the caller;
    2. the external id reported by the source system (Tilda ``tranid``, amoCRM entity id +
       modification time, Bitrix24 entity id + ``ts``);
    3. a SHA-256 of the raw body as a last resort.
    """
    suffix = f":{index}" if total > 1 else ""
    if header_key:
        return f"{source}:hdr:{header_key.strip()[:200]}{suffix}"
    if event.external_id:
        return f"{source}:ext:{event.type}:{event.external_id}"
    digest = hashlib.sha256(body).hexdigest()
    return f"{source}:body:{digest}{suffix}"
