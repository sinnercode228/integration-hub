"""Outbound connectors; importing the package registers them in ``base.OUTBOUND``."""

from relay.connectors.outbound.amocrm import AmoCrmLeadConnector
from relay.connectors.outbound.google_sheets import GoogleSheetsConnector
from relay.connectors.outbound.smtp import SmtpEmailConnector
from relay.connectors.outbound.telegram import TelegramConnector
from relay.connectors.outbound.webhook import WebhookConnector

__all__ = [
    "AmoCrmLeadConnector",
    "GoogleSheetsConnector",
    "SmtpEmailConnector",
    "TelegramConnector",
    "WebhookConnector",
]
