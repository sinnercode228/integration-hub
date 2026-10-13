"""Delivery queue contract.

Semantics (identical for the in-process and the Redis implementation):

* ``enqueue(job, delay)`` schedules a job; enqueuing the same job again only moves its due time.
* ``reserve`` atomically moves due jobs to *in-flight* with a lease. A worker that crashes
  loses its lease and ``requeue_expired`` makes the job visible again (at-least-once).
* The lease deadline doubles as a fencing token: ``reserve`` returns it in ``Job.lease``.
  ``extend``, ``ack``, ``retry`` and ``bury`` act only while the job is still in flight under
  that token, each in one atomic step, and report whether they did. A worker whose lease ran
  out and was taken over by another worker can therefore no longer move or drop the job.
  A job without a token (``lease=None``) is not fenced.
* ``dead_letter`` parks a job without touching *in-flight*; the worker uses ``bury``.
* ``enqueue_missing(jobs)`` adds only the jobs that are neither scheduled nor in flight and
  returns them. The check and the add are one atomic step, so it never moves a scheduled
  job's due time and never puts a second copy of a leased job in the queue. The worker's
  sweep uses it for deliveries that lost their job.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Job:
    delivery_id: str
    event_id: str
    # Lease deadline from ``reserve``/``extend``, used as a fencing token. Not part of the
    # job's identity and not encoded.
    lease: float | None = field(default=None, compare=False)

    def encode(self) -> str:
        return json.dumps({"d": self.delivery_id, "e": self.event_id}, sort_keys=True)

    @classmethod
    def decode(cls, raw: str | bytes) -> Job:
        data = json.loads(raw)
        return cls(delivery_id=str(data["d"]), event_id=str(data["e"]))


@dataclass(frozen=True, slots=True)
class DeadLetter:
    job: Job
    reason: str
    at: float


@dataclass(frozen=True, slots=True)
class QueueDepth:
    ready: int
    scheduled: int
    in_flight: int
    dead: int


class DeliveryQueue(Protocol):
    async def enqueue(self, job: Job, *, delay: float = 0.0) -> None: ...

    async def reserve(self, *, limit: int, lease_seconds: float) -> list[Job]: ...

    async def extend(self, job: Job, *, lease_seconds: float) -> Job | None: ...

    async def ack(self, job: Job) -> bool: ...

    async def retry(self, job: Job, *, delay: float) -> bool: ...

    async def bury(self, job: Job, reason: str) -> bool: ...

    async def requeue_expired(self) -> int: ...

    async def enqueue_missing(self, jobs: list[Job]) -> list[Job]: ...

    async def dead_letter(self, job: Job, reason: str) -> None: ...

    async def remove_dead(self, delivery_id: str) -> None: ...

    async def dead_letters(self, limit: int = 100) -> list[DeadLetter]: ...

    async def depth(self) -> QueueDepth: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...
