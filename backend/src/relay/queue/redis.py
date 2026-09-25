"""Redis-backed queue: sorted sets for scheduled / in-flight jobs, a hash for dead letters.

Keys (``<prefix>`` defaults to ``relay``)::

    <prefix>:q:scheduled   ZSET  member=job, score=due unix time
    <prefix>:q:inflight    ZSET  member=job, score=lease deadline
    <prefix>:q:dead        HASH  delivery_id -> {"job", "reason", "at"}

Claiming uses ``MULTI``: ``ZREM scheduled`` + ``ZADD inflight`` run atomically and a worker
only processes a job when *its own* ``ZREM`` removed it, so several worker processes can
share one Redis without double-processing.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from redis.asyncio import Redis

from relay.queue.base import DeadLetter, Job, QueueDepth


class RedisQueue:
    def __init__(
        self,
        client: Redis,
        *,
        prefix: str = "relay",
        clock: Callable[[], float] = time.time,
    ) -> None:
        self._redis = client
        self._clock = clock
        self._scheduled = f"{prefix}:q:scheduled"
        self._inflight = f"{prefix}:q:inflight"
        self._dead = f"{prefix}:q:dead"

    async def enqueue(self, job: Job, *, delay: float = 0.0) -> None:
        await self._redis.zadd(self._scheduled, {job.encode(): self._clock() + max(0.0, delay)})

    async def reserve(self, *, limit: int, lease_seconds: float) -> list[Job]:
        now = self._clock()
        members: list[Any] = await self._redis.zrangebyscore(
            self._scheduled, "-inf", now, start=0, num=limit
        )
        claimed: list[Job] = []
        for member in members:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.zrem(self._scheduled, member)
                pipe.zadd(self._inflight, {member: now + lease_seconds})
                removed, _ = await pipe.execute()
            if removed:
                claimed.append(Job.decode(member))
        return claimed

    async def ack(self, job: Job) -> None:
        await self._redis.zrem(self._inflight, job.encode())

    async def requeue_expired(self) -> int:
        now = self._clock()
        expired: list[Any] = await self._redis.zrangebyscore(self._inflight, "-inf", now)
        moved = 0
        for member in expired:
            async with self._redis.pipeline(transaction=True) as pipe:
                pipe.zrem(self._inflight, member)
                pipe.zadd(self._scheduled, {member: now}, nx=True)
                removed, _ = await pipe.execute()
            moved += int(bool(removed))
        return moved

    async def dead_letter(self, job: Job, reason: str) -> None:
        record = json.dumps({"job": job.encode(), "reason": reason, "at": self._clock()})
        await self._redis.hset(self._dead, job.delivery_id, record)

    async def remove_dead(self, delivery_id: str) -> None:
        await self._redis.hdel(self._dead, delivery_id)

    async def dead_letters(self, limit: int = 100) -> list[DeadLetter]:
        raw: dict[Any, Any] = await self._redis.hgetall(self._dead)
        items = []
        for value in raw.values():
            data = json.loads(value)
            items.append(
                DeadLetter(job=Job.decode(data["job"]), reason=data["reason"], at=data["at"])
            )
        items.sort(key=lambda item: item.at, reverse=True)
        return items[:limit]

    async def depth(self) -> QueueDepth:
        now = self._clock()
        async with self._redis.pipeline(transaction=False) as pipe:
            pipe.zcount(self._scheduled, "-inf", now)
            pipe.zcard(self._scheduled)
            pipe.zcard(self._inflight)
            pipe.hlen(self._dead)
            ready, scheduled_total, in_flight, dead = await pipe.execute()
        return QueueDepth(
            ready=int(ready),
            scheduled=int(scheduled_total) - int(ready),
            in_flight=int(in_flight),
            dead=int(dead),
        )

    async def ping(self) -> bool:
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    async def close(self) -> None:
        await self._redis.aclose()
