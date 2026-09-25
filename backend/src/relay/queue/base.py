"""Delivery queue contract.

Semantics (identical for the in-process and the Redis implementation):

* ``enqueue(job, delay)`` schedules a job; enqueuing the same job again only moves its due time.
* ``reserve`` atomically moves due jobs to *in-flight* with a lease. A worker that crashes
  loses its lease and ``requeue_expired`` makes the job visible again (at-least-once).
* ``ack`` removes an in-flight job. ``dead_letter`` parks a job that ran out of attempts.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class Job:
    delivery_id: str
    event_id: str

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

    async def ack(self, job: Job) -> None: ...

    async def requeue_expired(self) -> int: ...

    async def dead_letter(self, job: Job, reason: str) -> None: ...

    async def remove_dead(self, delivery_id: str) -> None: ...

    async def dead_letters(self, limit: int = 100) -> list[DeadLetter]: ...

    async def depth(self) -> QueueDepth: ...

    async def ping(self) -> bool: ...

    async def close(self) -> None: ...
