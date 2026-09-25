"""Connector plugins. Importing this package registers every built-in connector."""

from relay.connectors import inbound as _inbound  # noqa: F401  (registration side effect)
from relay.connectors import outbound as _outbound  # noqa: F401
from relay.connectors.base import (
    INBOUND,
    OUTBOUND,
    DeliveryContext,
    DeliveryOutcome,
    InboundConnector,
    InboundRequest,
    InboundResult,
    OutboundConnector,
)

__all__ = [
    "INBOUND",
    "OUTBOUND",
    "DeliveryContext",
    "DeliveryOutcome",
    "InboundConnector",
    "InboundRequest",
    "InboundResult",
    "OutboundConnector",
]
