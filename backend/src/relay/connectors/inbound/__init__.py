"""Inbound connectors; importing the package registers them in ``base.INBOUND``."""

from relay.connectors.inbound.amocrm import AmoCrmInbound
from relay.connectors.inbound.bitrix24 import Bitrix24Inbound
from relay.connectors.inbound.generic import GenericJsonInbound
from relay.connectors.inbound.tilda import TildaInbound

__all__ = ["AmoCrmInbound", "Bitrix24Inbound", "GenericJsonInbound", "TildaInbound"]
