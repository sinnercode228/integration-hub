"""Inbound webhook endpoint: ``POST /webhooks/{source_id}``."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse, PlainTextResponse, Response

from relay.api.deps import ContainerDep
from relay.connectors.base import InboundRequest
from relay.domain import utcnow
from relay.errors import PayloadError, SignatureError, UnknownSourceError

router = APIRouter(tags=["webhooks"])


async def read_body(request: Request, limit: int) -> bytes:
    declared = request.headers.get("content-length")
    if declared and declared.isdigit() and int(declared) > limit:
        raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "payload too large")
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            raise HTTPException(status.HTTP_413_CONTENT_TOO_LARGE, "payload too large")
        chunks.append(chunk)
    return b"".join(chunks)


@router.post(
    "/webhooks/{source_id}",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Receive a webhook from a configured source",
    responses={
        200: {"description": "Duplicate (already processed) or connection test"},
        401: {"description": "Signature / token verification failed"},
        404: {"description": "Unknown source"},
        413: {"description": "Body too large"},
        422: {"description": "Body cannot be parsed"},
    },
)
async def receive_webhook(source_id: str, request: Request, container: ContainerDep) -> Response:
    body = await read_body(request, container.settings.max_body_bytes)
    inbound = InboundRequest(
        body=body,
        headers={k.lower(): v for k, v in request.headers.items()},
        query=dict(request.query_params),
        content_type=request.headers.get("content-type", ""),
        received_at=utcnow(),
    )
    try:
        result = await container.ingest.ingest(
            source_id, inbound, idempotency_key=request.headers.get("idempotency-key")
        )
    except UnknownSourceError:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "unknown source") from None
    except SignatureError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "signature verification failed") from None
    except PayloadError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from None

    if result.ping:
        return PlainTextResponse("ok")
    code = status.HTTP_202_ACCEPTED if result.accepted else status.HTTP_200_OK
    return JSONResponse(
        {"accepted": result.accepted, "duplicates": result.duplicates}, status_code=code
    )
