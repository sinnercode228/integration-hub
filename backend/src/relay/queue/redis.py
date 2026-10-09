"""Redis-backed queue: sorted sets for scheduled / in-flight jobs, a hash for dead letters.

Keys (``<prefix>`` defaults to ``relay``)::

    <prefix>:q:scheduled   ZSET  member=job, score=due unix time
    <prefix>:q:inflight    ZSET  member=job, score=lease deadline
    <prefix>:q:dead        HASH  delivery_id -> {"job", "reason", "at"}

Claiming and requeueing each run as one Lua script: the members are read and moved inside
the same atomic step, so another worker can't change them in between. Reading in one round
trip and moving in a ``MULTI`` later let a second worker claim a retry before its backoff and
take back a lease that another worker had just taken (``tests/test_queue_races.py``).

Still open: ``ack`` is not fenced by the lease. A worker whose lease ran out can ack a job
that another worker has taken over, and removes that worker's lease.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any

from redis.asyncio import Redis

from relay.queue.base import DeadLetter, Job, QueueDepth

# KEYS: scheduled, inflight. ARGV: now, lease deadline, limit.
_RESERVE = """
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, ARGV[3])
for _, member in ipairs(due) do
  redis.call('ZREM', KEYS[1], member)
  redis.call('ZADD', KEYS[2], ARGV[2], member)
end
return due
"""

# KEYS: inflight, scheduled. ARGV: now. NX keeps a later due time set by a retry.
_REQUEUE_EXPIRED = """
local expired = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
for _, member in ipairs(expired) do
  redis.call('ZREM', KEYS[1], member)
  redis.call('ZADD', KEYS[2], 'NX', ARGV[1], member)
end
return #expired
"""


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
        self._reserve = client.register_script(_RESERVE)
        self._requeue_expired = client.register_script(_REQUEUE_EXPIRED)

    async def enqueue(self, job: Job, *, delay: float = 0.0) -> None:
        await self._redis.zadd(self._scheduled, {job.encode(): self._clock() + max(0.0, delay)})

    async def reserve(self, *, limit: int, lease_seconds: float) -> list[Job]:
        now = self._clock()
        members: list[Any] = await self._reserve(
            keys=[self._scheduled, self._inflight],
            args=[repr(now), repr(now + lease_seconds), limit],
        )
        return [Job.decode(member) for member in members]

    async def ack(self, job: Job) -> None:
        await self._redis.zrem(self._inflight, job.encode())

    async def requeue_expired(self) -> int:
        moved: int = await self._requeue_expired(
            keys=[self._inflight, self._scheduled], args=[repr(self._clock())]
        )
        return int(moved)

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
