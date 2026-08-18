"""Atomic tenant quota reservations covering all designed dimensions."""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass
from datetime import UTC, date, datetime

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


@dataclass(frozen=True)
class QuotaLimits:
    concurrent_jobs: int = 10
    daily_jobs: int = 1000
    daily_records: int = 10_000_000
    daily_bytes: int = 10_737_418_240
    per_job_compute_seconds: int = 3600
    per_job_model_tokens: int = 1_000_000
    retained_evidence_bytes: int = 10_485_760
    allowed_provider_classes: frozenset[str] = frozenset({"tabular"})


@dataclass(frozen=True)
class QuotaReservation:
    reservation_id: str
    tenant_id: str
    records: int
    bytes: int
    compute_seconds: int
    model_tokens: int
    provider_class: str
    evidence_bytes: int
    day: date

    def to_dict(self) -> dict[str, object]:
        return {
            "reservation_id": self.reservation_id,
            "tenant_id": self.tenant_id,
            "records": self.records,
            "bytes": self.bytes,
            "compute_seconds": self.compute_seconds,
            "model_tokens": self.model_tokens,
            "provider_class": self.provider_class,
            "evidence_bytes": self.evidence_bytes,
            "day": self.day.isoformat(),
        }


class InMemoryQuotaLedger:
    def __init__(self, limits: QuotaLimits | None = None) -> None:
        self.limits = limits or QuotaLimits()
        self._active: dict[str, QuotaReservation] = {}
        self._daily: dict[tuple[str, date], dict[str, int]] = {}
        self._lock = threading.RLock()

    def reserve(self, tenant_id: str, job_id: str, request: CreateJobRequest) -> QuotaReservation:
        bounds = request.generation_contract.bounds
        records = sum(item.count.maximum for item in request.generation_contract.records)
        compute = request.provider.requirements.maximum_runtime_seconds
        provider_class = request.provider.provider_class
        day = datetime.now(UTC).date()
        limits = self.limits
        with self._lock:
            concurrent = sum(item.tenant_id == tenant_id for item in self._active.values())
            daily = self._daily.setdefault((tenant_id, day), {"jobs": 0, "records": 0, "bytes": 0})
            violated = (
                concurrent >= limits.concurrent_jobs
                or daily["jobs"] + 1 > limits.daily_jobs
                or daily["records"] + records > limits.daily_records
                or daily["bytes"] + bounds.max_bytes > limits.daily_bytes
                or compute > limits.per_job_compute_seconds
                or provider_class not in limits.allowed_provider_classes
            )
            if violated:
                raise SyntheticDataError(
                    ErrorCode.QUOTA_EXCEEDED,
                    "requested generation exceeds tenant quota",
                    retryable=True,
                )
            reservation_id = (
                "qres_"
                + hashlib.sha256(f"{tenant_id}:{job_id}:{request.digest}".encode()).hexdigest()[:24]
            )
            reservation = QuotaReservation(
                reservation_id=reservation_id,
                tenant_id=tenant_id,
                records=records,
                bytes=bounds.max_bytes,
                compute_seconds=compute,
                model_tokens=0,
                provider_class=provider_class,
                evidence_bytes=min(1_048_576, limits.retained_evidence_bytes),
                day=day,
            )
            self._active[reservation_id] = reservation
            daily["jobs"] += 1
            daily["records"] += records
            daily["bytes"] += bounds.max_bytes
            return reservation

    def release(self, reservation_id: str) -> None:
        with self._lock:
            self._active.pop(reservation_id, None)

    @property
    def active_count(self) -> int:
        with self._lock:
            return len(self._active)
