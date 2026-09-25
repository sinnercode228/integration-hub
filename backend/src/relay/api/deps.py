"""FastAPI dependencies."""

from __future__ import annotations

from typing import Annotated

from fastapi import Depends, HTTPException, Request, status

from relay.container import Container
from relay.security import secure_compare


def get_container(request: Request) -> Container:
    container: Container | None = getattr(request.app.state, "container", None)
    if container is None:  # pragma: no cover - only before lifespan startup
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "service is starting")
    return container


ContainerDep = Annotated[Container, Depends(get_container)]


def require_admin(request: Request, container: ContainerDep) -> None:
    settings = container.settings
    if settings.admin_token is None:
        if settings.env == "prod":
            raise HTTPException(
                status.HTTP_503_SERVICE_UNAVAILABLE, "admin API is disabled: set RELAY_ADMIN_TOKEN"
            )
        return  # dev/test without a token: open admin API on localhost
    header = request.headers.get("authorization", "")
    scheme, _, token = header.partition(" ")
    if scheme.lower() != "bearer" or not secure_compare(
        token.strip(), settings.admin_token.get_secret_value()
    ):
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "invalid admin token",
            headers={"WWW-Authenticate": "Bearer"},
        )


AdminDep = Depends(require_admin)
