"""Admin API used by the dashboard: events, statuses, dead letters, replay."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, HTTPException, Query, status

from relay.api.deps import AdminDep, ContainerDep
from relay.api.schemas import (
    ConnectorsResponse,
    DeadLetterItem,
    EventPage,
    EventSummary,
    QueueInfo,
    ReplayResponse,
    StatsResponse,
)
from relay.connectors import INBOUND, OUTBOUND
from relay.domain import DeliveryRecord, DeliveryStatus, EventRecord, EventStatus
from relay.store.base import EventFilter

router = APIRouter(prefix="/admin/api", tags=["admin"], dependencies=[AdminDep])


@router.get("/stats")
async def stats(container: ContainerDep) -> StatsResponse:
    store_stats = await container.store.stats()
    depth = await container.queue.depth()
    return StatsResponse(
        events_total=store_stats.events_total,
        events_by_status=store_stats.events_by_status,
        events_by_source=store_stats.events_by_source,
        deliveries_by_status=store_stats.deliveries_by_status,
        deliveries_by_destination=store_stats.deliveries_by_destination,
        queue=QueueInfo(
            ready=depth.ready, scheduled=depth.scheduled, in_flight=depth.in_flight, dead=depth.dead
        ),
    )


@router.get("/events")
async def list_events(
    container: ContainerDep,
    status_: Annotated[EventStatus | None, Query(alias="status")] = None,
    source: str | None = None,
    type_: Annotated[str | None, Query(alias="type")] = None,
    q: Annotated[str | None, Query(max_length=100)] = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    offset: Annotated[int, Query(ge=0)] = 0,
) -> EventPage:
    flt = EventFilter(
        status=status_, source=source, type=type_, query=q, limit=limit, offset=offset
    )
    events, total = await container.store.list_events(flt)
    return EventPage(
        items=[EventSummary.of(e) for e in events], total=total, limit=limit, offset=offset
    )


@router.get("/events/{event_id}")
async def get_event(event_id: str, container: ContainerDep) -> EventRecord:
    event = await container.store.get_event(event_id)
    if event is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "event not found")
    return event


@router.post("/events/{event_id}/replay")
async def replay_event(
    event_id: str,
    container: ContainerDep,
    include_delivered: bool = False,
) -> ReplayResponse:
    event = await container.store.get_event(event_id)
    if event is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "event not found")
    replayable = {DeliveryStatus.DEAD} | (
        {DeliveryStatus.DELIVERED} if include_delivered else set()
    )
    response = ReplayResponse(replayed=[], skipped=[])
    for delivery in event.deliveries:
        if delivery.status in replayable:
            await container.worker.replay(delivery)
            response.replayed.append(delivery.id)
        else:
            response.skipped.append(delivery.id)
    return response


@router.post("/deliveries/{delivery_id}/replay")
async def replay_delivery(delivery_id: str, container: ContainerDep) -> DeliveryRecord:
    delivery = await container.store.get_delivery(delivery_id)
    if delivery is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "delivery not found")
    if not delivery.is_final:
        raise HTTPException(status.HTTP_409_CONFLICT, "delivery is still in progress")
    return await container.worker.replay(delivery)


@router.get("/dead-letters")
async def dead_letters(
    container: ContainerDep, limit: Annotated[int, Query(ge=1, le=500)] = 100
) -> list[DeadLetterItem]:
    deliveries = await container.store.list_deliveries(DeliveryStatus.DEAD, limit=limit)
    items: list[DeadLetterItem] = []
    cache: dict[str, EventRecord | None] = {}
    for delivery in deliveries:
        if delivery.event_id not in cache:
            cache[delivery.event_id] = await container.store.get_event(delivery.event_id)
        event = cache[delivery.event_id]
        items.append(
            DeadLetterItem(delivery=delivery, event=EventSummary.of(event) if event else None)
        )
    return items


@router.get("/connectors")
async def connectors(container: ContainerDep) -> ConnectorsResponse:
    summary = container.config.public_summary()
    return ConnectorsResponse(
        **summary,
        available={
            "inbound": [{"kind": k, "label": c.label} for k, c in sorted(INBOUND.items())],
            "outbound": [{"kind": k, "label": c.label} for k, c in sorted(OUTBOUND.items())],
        },
        retry_schedule_seconds=[round(s, 1) for s in container.worker.policy.schedule()],
    )
