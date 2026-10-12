"""Redis-backed queue: sorted sets for scheduled / in-flight jobs, a hash for dead letters.

Keys (``<prefix>`` defaults to ``relay``)::

    <prefix>:q:scheduled   ZSET  member=job, score=due unix time
    <prefix>:q:inflight    ZSET  member=job, score=lease deadline
    <prefix>:q:dead        HASH  delivery_id -> {"job", "reason", "at"}

Every operation that reads a member and then moves it runs as one Lua script, so another
worker can't change the member in between. Reading in one round trip and moving in a
``MULTI`` later let a second worker claim a retry before its backoff and take back a lease
that another worker had just taken (``tests/test_queue_races.py``).

The ``inflight`` score is the lease deadline, and it doubles as a fencing token: ``reserve``
and ``extend`` return it in ``Job.lease``, and ``extend``, ``ack``, ``retry`` and ``bury``
act only if the member's score still equals it. A worker whose lease ran out and was taken
over gets ``False``/``None`` back instead of dropping or moving the new owner's job.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable
from dataclasses import replace
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

# Shared head of the fenced scripts. KEYS[1]: inflight. ARGV[1]: member, ARGV[2]: the lease
# token ("" = not fenced; extend never gets "", it returns early for a job without a token).
_FENCE = """
local score = redis.call('ZSCORE', KEYS[1], ARGV[1])
if ARGV[2] ~= '' and (not score or tonumber(score) ~= tonumber(ARGV[2])) then
  return 0
end
"""

# ARGV[3]: new lease deadline.
_EXTEND = (
    _FENCE
    + """
redis.call('ZADD', KEYS[1], 'XX', ARGV[3], ARGV[1])
return 1
"""
)

_ACK = (
    _FENCE
    + """
redis.call('ZREM', KEYS[1], ARGV[1])
return 1
"""
)

# KEYS[2]: scheduled. ARGV[3]: due time.
_RETRY = (
    _FENCE
    + """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('ZADD', KEYS[2], ARGV[3], ARGV[1])
return 1
"""
)

# KEYS[2]: dead. ARGV[3]: delivery id, ARGV[4]: dead-letter record.
_BURY = (
    _FENCE
    + """
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HSET', KEYS[2], ARGV[3], ARGV[4])
return 1
"""
)


def _token(job: Job) -> str:
    return "" if job.lease is None else repr(job.lease)


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
        self._extend = client.register_script(_EXTEND)
        self._ack = client.register_script(_ACK)
        self._retry = client.register_script(_RETRY)
        self._bury = client.register_script(_BURY)

    async def enqueue(self, job: Job, *, delay: float = 0.0) -> None:
        await self._redis.zadd(self._scheduled, {job.encode(): self._clock() + max(0.0, delay)})

    async def reserve(self, *, limit: int, lease_seconds: float) -> list[Job]:
        now = self._clock()
        deadline = now + lease_seconds
        members: list[Any] = await self._reserve(
            keys=[self._scheduled, self._inflight],
            args=[repr(now), repr(deadline), limit],
        )
        return [replace(Job.decode(member), lease=deadline) for member in members]

    async def extend(self, job: Job, *, lease_seconds: float) -> Job | None:
        if job.lease is None:
            return job
        deadline = self._clock() + lease_seconds
        done = await self._extend(
            keys=[self._inflight], args=[job.encode(), _token(job), repr(deadline)]
        )
        return replace(job, lease=deadline) if done else None

    async def ack(self, job: Job) -> bool:
        done = await self._ack(keys=[self._inflight], args=[job.encode(), _token(job)])
        return bool(done)

    async def retry(self, job: Job, *, delay: float) -> bool:
        due = self._clock() + max(0.0, delay)
        done = await self._retry(
            keys=[self._inflight, self._scheduled], args=[job.encode(), _token(job), repr(due)]
        )
        return bool(done)

    async def bury(self, job: Job, reason: str) -> bool:
        record = json.dumps({"job": job.encode(), "reason": reason, "at": self._clock()})
        done = await self._bury(
            keys=[self._inflight, self._dead],
            args=[job.encode(), _token(job), job.delivery_id, record],
        )
        return bool(done)

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
