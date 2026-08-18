"""Tenant-scoped optimistic job persistence contract and in-memory implementation."""

from __future__ import annotations

import copy
import threading
from typing import Protocol

from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

from .models import Job


class JobRepository(Protocol):
    def create(self, job: Job) -> Job: ...

    def get(self, tenant_id: str, job_id: str) -> Job | None: ...

    def find_idempotent(self, tenant_id: str, idempotency_key: str) -> Job | None: ...

    def save(self, job: Job, *, expected_version: int) -> Job: ...

    def list_runnable(self, *, limit: int = 100) -> tuple[Job, ...]: ...


class InMemoryJobRepository:
    """Deterministic repository used by unit tests and local process composition."""

    def __init__(self) -> None:
        self._jobs: dict[tuple[str, str], Job] = {}
        self._idempotency: dict[tuple[str, str], str] = {}
        self._lock = threading.RLock()

    def create(self, job: Job) -> Job:
        with self._lock:
            identity = (job.tenant_id, job.job_id)
            idem = (job.tenant_id, job.idempotency_key)
            if identity in self._jobs or idem in self._idempotency:
                raise SyntheticDataError(ErrorCode.CONCURRENCY_CONFLICT, "job already exists")
            self._jobs[identity] = copy.deepcopy(job)
            self._idempotency[idem] = job.job_id
            return copy.deepcopy(job)

    def get(self, tenant_id: str, job_id: str) -> Job | None:
        with self._lock:
            job = self._jobs.get((tenant_id, job_id))
            return copy.deepcopy(job) if job is not None else None

    def find_idempotent(self, tenant_id: str, idempotency_key: str) -> Job | None:
        with self._lock:
            job_id = self._idempotency.get((tenant_id, idempotency_key))
            return self.get(tenant_id, job_id) if job_id is not None else None

    def save(self, job: Job, *, expected_version: int) -> Job:
        with self._lock:
            identity = (job.tenant_id, job.job_id)
            current = self._jobs.get(identity)
            if current is None:
                raise SyntheticDataError(ErrorCode.JOB_NOT_FOUND, "job not found")
            if current.version != expected_version:
                raise SyntheticDataError(
                    ErrorCode.CONCURRENCY_CONFLICT,
                    "job version changed during update",
                    retryable=True,
                )
            self._jobs[identity] = copy.deepcopy(job)
            return copy.deepcopy(job)

    def list_runnable(self, *, limit: int = 100) -> tuple[Job, ...]:
        with self._lock:
            jobs = sorted(self._jobs.values(), key=lambda item: (item.created_at, item.job_id))
            return tuple(copy.deepcopy(job) for job in jobs[:limit] if not job.terminal)
