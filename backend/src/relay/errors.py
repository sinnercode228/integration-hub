"""Exception hierarchy. Every error raised on purpose inside Relay is a ``RelayError``."""

from __future__ import annotations


class RelayError(Exception):
    """Base class for all expected Relay errors."""


class ConfigError(RelayError):
    """The YAML configuration or the environment is invalid."""


class UnknownSourceError(RelayError):
    """A webhook arrived for a source id that is not configured."""


class SignatureError(RelayError):
    """The inbound request could not be authenticated (bad or missing signature/token)."""


class PayloadError(RelayError):
    """The inbound request is authentic but its body cannot be parsed."""


class TemplateError(RelayError):
    """A mapping template could not be rendered."""


class UpstreamUnavailableError(RelayError):
    """An upstream API used synchronously (e.g. the stock proxy) is unavailable."""


class DeliveryError(RelayError):
    """An outbound delivery attempt failed.

    ``retryable`` decides whether the job goes back to the retry queue or straight to the
    dead-letter queue. ``retry_after`` lets a connector honour server hints such as HTTP
    ``Retry-After`` or Telegram's ``parameters.retry_after``.
    """

    def __init__(
        self,
        message: str,
        *,
        retryable: bool,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.retryable = retryable
        self.status_code = status_code
        self.retry_after = retry_after

    def __repr__(self) -> str:
        return (
            f"DeliveryError({self.message!r}, retryable={self.retryable}, "
            f"status_code={self.status_code}, retry_after={self.retry_after})"
        )
