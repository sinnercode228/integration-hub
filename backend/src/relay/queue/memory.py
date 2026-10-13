"""In-process queue: zero dependencies, used for local dev and tests."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable
from dataclasses import replace

from relay.queue.base import DeadLetter, Job, QueueDepth


class InMemoryQueue:
    def __init__(self, clock: Callable[[], float] = time.time) -> None:
        self._clock = clock
        self._scheduled: dict[Job, float] = {}
        self._in_flight: dict[Job, float] = {}
        self._dead: dict[str, DeadLetter] = {}
        self._lock = asyncio.Lock()

    async def enqueue(self, job: Job, *, delay: float = 0.0) -> None:
        async with self._lock:
            self._scheduled[job] = self._clock() + max(0.0, delay)

    async def reserve(self, *, limit: int, lease_seconds: float) -> list[Job]:
        async with self._lock:
            now = self._clock()
            due = sorted(
                (at, job.delivery_id, job) for job, at in self._scheduled.items() if at <= now
            )
            taken = [job for _, _, job in due[:limit]]
            deadline = now + lease_seconds
            for job in taken:
                del self._scheduled[job]
                self._in_flight[job] = deadline
            return [replace(job, lease=deadline) for job in taken]

    def _holds(self, job: Job) -> bool:
        deadline = self._in_flight.get(job)
        return deadline is not None and (job.lease is None or deadline == job.lease)

    async def extend(self, job: Job, *, lease_seconds: float) -> Job | None:
        async with self._lock:
            if job.lease is None:
                return job
            if not self._holds(job):
                return None
            deadline = self._clock() + lease_seconds
            self._in_flight[job] = deadline
            return replace(job, lease=deadline)

    async def ack(self, job: Job) -> bool:
        async with self._lock:
            if job.lease is not None and not self._holds(job):
                return False
            self._in_flight.pop(job, None)
            return True

    async def retry(self, job: Job, *, delay: float) -> bool:
        async with self._lock:
            if job.lease is not None and not self._holds(job):
                return False
            self._in_flight.pop(job, None)
            self._scheduled[replace(job, lease=None)] = self._clock() + max(0.0, delay)
            return True

    async def bury(self, job: Job, reason: str) -> bool:
        async with self._lock:
            if job.lease is not None and not self._holds(job):
                return False
            self._in_flight.pop(job, None)
            parked = replace(job, lease=None)
            self._dead[job.delivery_id] = DeadLetter(job=parked, reason=reason, at=self._clock())
            return True

    async def requeue_expired(self) -> int:
        async with self._lock:
            now = self._clock()
            expired = [job for job, deadline in self._in_flight.items() if deadline <= now]
            for job in expired:
                del self._in_flight[job]
                self._scheduled.setdefault(job, now)
            return len(expired)

    async def enqueue_missing(self, jobs: list[Job]) -> list[Job]:
        async with self._lock:
            now = self._clock()
            added = []
            for job in jobs:
                bare = replace(job, lease=None)
                if bare not in self._scheduled and bare not in self._in_flight:
                    self._scheduled[bare] = now
                    added.append(bare)
            return added

    async def dead_letter(self, job: Job, reason: str) -> None:
        async with self._lock:
            self._dead[job.delivery_id] = DeadLetter(job=job, reason=reason, at=self._clock())

    async def remove_dead(self, delivery_id: str) -> None:
        async with self._lock:
            self._dead.pop(delivery_id, None)

    async def dead_letters(self, limit: int = 100) -> list[DeadLetter]:
        items = sorted(self._dead.values(), key=lambda item: item.at, reverse=True)
        return items[:limit]

    async def depth(self) -> QueueDepth:
        now = self._clock()
        ready = sum(1 for at in self._scheduled.values() if at <= now)
        return QueueDepth(
            ready=ready,
            scheduled=len(self._scheduled) - ready,
            in_flight=len(self._in_flight),
            dead=len(self._dead),
        )

    async def ping(self) -> bool:
        return True

    async def close(self) -> None:
        return None
