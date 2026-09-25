"""SQLite event store (stdlib ``sqlite3`` in a worker thread, WAL mode).

Good enough for a single-node deployment handling thousands of webhooks per minute; the
``EventStore`` protocol keeps the door open for Postgres without touching the pipeline.
"""

from __future__ import annotations

import asyncio
import sqlite3
import threading
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar

from relay.domain import DeliveryRecord, DeliveryStatus, EventRecord, EventStatus, aggregate_status
from relay.store.base import EventFilter, StoreStats

T = TypeVar("T")

_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id TEXT PRIMARY KEY,
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    status TEXT NOT NULL,
    received_at TEXT NOT NULL,
    search TEXT NOT NULL,
    doc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_events_received ON events (received_at DESC, id DESC);
CREATE INDEX IF NOT EXISTS ix_events_status ON events (status);
CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL REFERENCES events (id) ON DELETE CASCADE,
    destination TEXT NOT NULL,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    doc TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ix_deliveries_event ON deliveries (event_id);
CREATE INDEX IF NOT EXISTS ix_deliveries_status ON deliveries (status, updated_at DESC);
"""


def _search_blob(event: EventRecord) -> str:
    parts = [
        event.id,
        event.external_id or "",
        event.type,
        event.contact.name or "",
        event.contact.phone or "",
        event.contact.email or "",
    ]
    return " ".join(parts).lower()


class SqliteEventStore:
    def __init__(self, path: Path | str) -> None:
        self._path = str(path)
        if self._path != ":memory:":
            Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(_SCHEMA)

    async def _run(self, fn: Callable[[sqlite3.Connection], T]) -> T:
        def call() -> T:
            with self._lock:
                return fn(self._conn)

        return await asyncio.to_thread(call)

    # -- writes ---------------------------------------------------------------------------

    async def add_event(self, event: EventRecord) -> None:
        doc = event.model_dump_json(exclude={"deliveries"})
        rows = [
            (
                d.id,
                d.event_id,
                d.destination,
                d.status.value,
                d.updated_at.isoformat(),
                d.model_dump_json(),
            )
            for d in event.deliveries
        ]

        def write(conn: sqlite3.Connection) -> None:
            conn.execute("BEGIN")
            try:
                conn.execute(
                    "INSERT INTO events (id, source, type, status, received_at, search, doc)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        event.id,
                        event.source,
                        event.type,
                        event.status.value,
                        event.received_at.isoformat(),
                        _search_blob(event),
                        doc,
                    ),
                )
                conn.executemany(
                    "INSERT INTO deliveries (id, event_id, destination, status, updated_at, doc)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    rows,
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise

        await self._run(write)

    async def save_delivery(self, delivery: DeliveryRecord) -> EventStatus:
        def write(conn: sqlite3.Connection) -> EventStatus:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE deliveries SET status = ?, updated_at = ?, doc = ? WHERE id = ?",
                    (
                        delivery.status.value,
                        delivery.updated_at.isoformat(),
                        delivery.model_dump_json(),
                        delivery.id,
                    ),
                )
                docs = conn.execute(
                    "SELECT doc FROM deliveries WHERE event_id = ?", (delivery.event_id,)
                ).fetchall()
                status = aggregate_status(
                    DeliveryRecord.model_validate_json(row["doc"]) for row in docs
                )
                row = conn.execute(
                    "SELECT doc FROM events WHERE id = ?", (delivery.event_id,)
                ).fetchone()
                if row is not None:
                    event = EventRecord.model_validate_json(row["doc"])
                    event.status = status
                    conn.execute(
                        "UPDATE events SET status = ?, doc = ? WHERE id = ?",
                        (status.value, event.model_dump_json(exclude={"deliveries"}), event.id),
                    )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return status

        return await self._run(write)

    # -- reads ----------------------------------------------------------------------------

    @staticmethod
    def _load_deliveries(
        conn: sqlite3.Connection, event_ids: list[str]
    ) -> dict[str, list[DeliveryRecord]]:
        grouped: dict[str, list[DeliveryRecord]] = defaultdict(list)
        if not event_ids:
            return grouped
        placeholders = ",".join("?" for _ in event_ids)
        rows = conn.execute(
            f"SELECT event_id, doc FROM deliveries WHERE event_id IN ({placeholders})"  # noqa: S608
            " ORDER BY rowid",
            event_ids,
        ).fetchall()
        for row in rows:
            grouped[row["event_id"]].append(DeliveryRecord.model_validate_json(row["doc"]))
        return grouped

    async def get_event(self, event_id: str) -> EventRecord | None:
        def read(conn: sqlite3.Connection) -> EventRecord | None:
            row = conn.execute("SELECT doc FROM events WHERE id = ?", (event_id,)).fetchone()
            if row is None:
                return None
            event = EventRecord.model_validate_json(row["doc"])
            event.deliveries = self._load_deliveries(conn, [event_id]).get(event_id, [])
            return event

        return await self._run(read)

    async def list_events(self, flt: EventFilter) -> tuple[list[EventRecord], int]:
        clauses: list[str] = []
        params: list[Any] = []
        if flt.status is not None:
            clauses.append("status = ?")
            params.append(flt.status.value)
        if flt.source:
            clauses.append("source = ?")
            params.append(flt.source)
        if flt.type:
            clauses.append("type = ?")
            params.append(flt.type)
        if flt.query:
            clauses.append("search LIKE ? ESCAPE '\\'")
            escaped = (
                flt.query.lower().replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
            )
            params.append(f"%{escaped}%")
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

        def read(conn: sqlite3.Connection) -> tuple[list[EventRecord], int]:
            total = conn.execute(f"SELECT COUNT(*) FROM events {where}", params).fetchone()[0]  # noqa: S608
            rows = conn.execute(
                f"SELECT doc FROM events {where} ORDER BY received_at DESC, id DESC"  # noqa: S608
                " LIMIT ? OFFSET ?",
                [*params, flt.limit, flt.offset],
            ).fetchall()
            events = [EventRecord.model_validate_json(row["doc"]) for row in rows]
            deliveries = self._load_deliveries(conn, [e.id for e in events])
            for event in events:
                event.deliveries = deliveries.get(event.id, [])
            return events, int(total)

        return await self._run(read)

    async def get_delivery(self, delivery_id: str) -> DeliveryRecord | None:
        def read(conn: sqlite3.Connection) -> DeliveryRecord | None:
            row = conn.execute("SELECT doc FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
            return DeliveryRecord.model_validate_json(row["doc"]) if row else None

        return await self._run(read)

    async def list_deliveries(
        self, status: DeliveryStatus | None = None, limit: int = 100
    ) -> list[DeliveryRecord]:
        def read(conn: sqlite3.Connection) -> list[DeliveryRecord]:
            if status is None:
                rows = conn.execute(
                    "SELECT doc FROM deliveries ORDER BY updated_at DESC LIMIT ?", (limit,)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT doc FROM deliveries WHERE status = ? ORDER BY updated_at DESC LIMIT ?",
                    (status.value, limit),
                ).fetchall()
            return [DeliveryRecord.model_validate_json(row["doc"]) for row in rows]

        return await self._run(read)

    async def stats(self) -> StoreStats:
        def read(conn: sqlite3.Connection) -> StoreStats:
            stats = StoreStats()
            stats.events_total = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            stats.events_by_status = {
                r[0]: r[1]
                for r in conn.execute("SELECT status, COUNT(*) FROM events GROUP BY status")
            }
            stats.events_by_source = {
                r[0]: r[1]
                for r in conn.execute("SELECT source, COUNT(*) FROM events GROUP BY source")
            }
            per_destination: dict[str, dict[str, int]] = defaultdict(dict)
            totals: dict[str, int] = defaultdict(int)
            for destination, status, count in conn.execute(
                "SELECT destination, status, COUNT(*) FROM deliveries GROUP BY destination, status"
            ):
                per_destination[destination][status] = count
                totals[status] += count
            stats.deliveries_by_status = dict(totals)
            stats.deliveries_by_destination = dict(per_destination)
            return stats

        return await self._run(read)

    async def ping(self) -> bool:
        def check(conn: sqlite3.Connection) -> bool:
            row = conn.execute("SELECT 1").fetchone()
            return bool(row is not None and row[0] == 1)

        return await self._run(check)

    async def close(self) -> None:
        def close(conn: sqlite3.Connection) -> None:
            conn.close()

        await self._run(close)
