"""Stable public failures for the generation service."""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    CONTRACT_INVALID = "CONTRACT_INVALID"
    PROVIDER_UNSUPPORTED = "PROVIDER_UNSUPPORTED"
    POLICY_DENIED = "POLICY_DENIED"
    OUTPUT_LIMIT_EXCEEDED = "OUTPUT_LIMIT_EXCEEDED"
    DESTINATION_INVALID = "DESTINATION_INVALID"
    DESTINATION_FORBIDDEN = "DESTINATION_FORBIDDEN"
    DESTINATION_CONFLICT = "DESTINATION_CONFLICT"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"
    GENERATION_NOT_FOUND = "GENERATION_NOT_FOUND"
    DELETE_CONFLICT = "DELETE_CONFLICT"


class SyntheticDataError(Exception):
    """A bounded, stable error suitable for persistence and API projection."""

    def __init__(
        self,
        code: ErrorCode,
        message: str,
        *,
        retryable: bool = False,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message[:500]
        self.retryable = retryable
        safe = details or {}
        self.details = {str(key)[:80]: str(value)[:500] for key, value in safe.items()}

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code.value,
            "message": self.message,
            "retryable": self.retryable,
            "details": dict(self.details),
        }
