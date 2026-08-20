"""Stable public failures for the generation service."""

from __future__ import annotations

from enum import StrEnum
from typing import Any


class ErrorCode(StrEnum):
    CONTRACT_INVALID = "CONTRACT_INVALID"
    PROVIDER_UNSUPPORTED = "PROVIDER_UNSUPPORTED"
    POLICY_DENIED = "POLICY_DENIED"
    QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    DETERMINISTIC_SEED_INCOMPATIBLE = "DETERMINISTIC_SEED_INCOMPATIBLE"
    VALIDATOR_FAILED = "VALIDATOR_FAILED"
    SANDBOX_VIOLATION = "SANDBOX_VIOLATION"
    OUTPUT_LIMIT_EXCEEDED = "OUTPUT_LIMIT_EXCEEDED"
    PUBLICATION_FAILED = "PUBLICATION_FAILED"
    DEPENDENCY_UNAVAILABLE = "DEPENDENCY_UNAVAILABLE"
    DEPENDENCY_DEADLINE = "DEPENDENCY_DEADLINE"
    DELIVERY_EXHAUSTED = "DELIVERY_EXHAUSTED"
    IDEMPOTENCY_KEY_REUSED = "IDEMPOTENCY_KEY_REUSED"
    JOB_CANCELLED = "JOB_CANCELLED"
    JOB_NOT_FOUND = "JOB_NOT_FOUND"
    JOB_NOT_SUCCEEDED = "JOB_NOT_SUCCEEDED"
    CONCURRENCY_CONFLICT = "CONCURRENCY_CONFLICT"


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
