"""Tenant-scoped optimistic job persistence contract and in-memory implementation."""

from __future__ import annotations

import copy
import threading
import uuid
from datetime import datetime, timedelta
from typing import Any, Protocol

from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.relay.models import (
    DeadLetterRecord,
    OutboxLease,
    dead_letter_record_digest,
)

from .models import Job


class JobRepository(Protocol):
    def create(self, job: Job) -> Job: ...

    def get(self, tenant_id: str, job_id: str) -> Job | None: ...

    def find_idempotent(self, tenant_id: str, idempotency_key: str) -> Job | None: ...

    def save(self, job: Job, *, expected_version: int) -> Job: ...

    def list_runnable(self, *, limit: int = 100) -> tuple[Job, ...]: ...

    def save_with_dispatch(
        self,
        job: Job,
        *,
        expected_version: int,
        input_artifact: Any,
        outbox: Any,
    ) -> Job: ...

    def save_with_control(self, job: Job, *, expected_version: int, outbox: Any) -> Job: ...

    def worker_event_digest(self, event_id: str) -> str | None: ...

    def worker_attempt_exists(self, tenant_id: str, job_id: str, attempt_id: str) -> bool: ...

    def commit_worker_event(
        self, job: Job, *, expected_version: int, event: Any, disposition: str
    ) -> Job: ...

    def lease_outbox(
        self,
        *,
        relay_id: str,
        limit: int,
        now: datetime,
        lease_seconds: int,
    ) -> tuple[OutboxLease, ...]: ...

    def renew_outbox_lease(
        self,
        message_id: str,
        lease_token: str,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> bool: ...

    def mark_outbox_published(
        self,
        message_id: str,
        lease_token: str,
        *,
        publication_id: str,
        published_at: datetime,
    ) -> bool: ...

    def release_outbox_lease(
        self,
        message_id: str,
        lease_token: str,
        *,
        next_attempt_at: datetime,
        error_code: str,
    ) -> bool: ...

    def dead_letter_digest(self, dead_letter_id: str) -> str | None: ...

    def commit_dead_letter(
        self,
        job: Job,
        *,
        expected_version: int,
        record: DeadLetterRecord,
        disposition: str,
    ) -> Job: ...


class InMemoryJobRepository:
    """Deterministic repository used by unit tests and local process composition."""

    def __init__(self) -> None:
        self._jobs: dict[tuple[str, str], Job] = {}
        self._idempotency: dict[tuple[str, str], str] = {}
        self._outbox: dict[str, Any] = {}
        self._outbox_state: dict[str, dict[str, Any]] = {}
        self._inputs: dict[str, Any] = {}
        self._inbox: dict[str, tuple[str, str]] = {}
        self._dead_letters: dict[str, tuple[str, str]] = {}
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

    def save_with_dispatch(
        self,
        job: Job,
        *,
        expected_version: int,
        input_artifact: Any,
        outbox: Any,
    ) -> Job:
        with self._lock:
            if outbox.message_id in self._outbox:
                raise SyntheticDataError(ErrorCode.CONCURRENCY_CONFLICT, "outbox identity exists")
            saved = self.save(job, expected_version=expected_version)
            self._inputs[job.active_attempt_id or ""] = copy.deepcopy(input_artifact)
            self._outbox[outbox.message_id] = copy.deepcopy(outbox)
            self._outbox_state[outbox.message_id] = {
                "published_at": None,
                "publication_id": None,
                "lease_owner": None,
                "lease_token": None,
                "lease_expires_at": None,
                "publish_attempts": 0,
                "next_attempt_at": datetime.min.replace(tzinfo=job.updated_at.tzinfo),
                "last_error_code": None,
            }
            return saved

    def save_with_control(self, job: Job, *, expected_version: int, outbox: Any) -> Job:
        with self._lock:
            prior = self._outbox.get(outbox.message_id)
            if prior is not None and prior.content_digest != outbox.content_digest:
                raise SyntheticDataError(ErrorCode.CONCURRENCY_CONFLICT, "outbox digest conflict")
            saved = self.save(job, expected_version=expected_version)
            self._outbox[outbox.message_id] = copy.deepcopy(outbox)
            self._outbox_state.setdefault(
                outbox.message_id,
                {
                    "published_at": None,
                    "publication_id": None,
                    "lease_owner": None,
                    "lease_token": None,
                    "lease_expires_at": None,
                    "publish_attempts": 0,
                    "next_attempt_at": datetime.min.replace(tzinfo=job.updated_at.tzinfo),
                    "last_error_code": None,
                },
            )
            return saved

    def worker_event_digest(self, event_id: str) -> str | None:
        with self._lock:
            value = self._inbox.get(event_id)
            return value[0] if value else None

    def worker_attempt_exists(self, tenant_id: str, job_id: str, attempt_id: str) -> bool:
        with self._lock:
            job = self._jobs.get((tenant_id, job_id))
            return bool(job and (attempt_id in self._inputs or job.active_attempt_id == attempt_id))

    def commit_worker_event(
        self, job: Job, *, expected_version: int, event: Any, disposition: str
    ) -> Job:
        with self._lock:
            prior = self._inbox.get(event.event_id)
            if prior is not None:
                if prior[0] != event.content_digest:
                    raise SyntheticDataError(
                        ErrorCode.CONCURRENCY_CONFLICT, "worker event digest conflict"
                    )
                return self.get(job.tenant_id, job.job_id) or job
            saved = self.save(job, expected_version=expected_version)
            self._inbox[event.event_id] = (event.content_digest, disposition)
            return saved

    def pending_outbox(self) -> tuple[Any, ...]:
        with self._lock:
            return tuple(
                copy.deepcopy(self._outbox[key])
                for key in sorted(self._outbox)
                if self._outbox_state[key]["published_at"] is None
            )

    def lease_outbox(
        self,
        *,
        relay_id: str,
        limit: int,
        now: datetime,
        lease_seconds: int,
    ) -> tuple[OutboxLease, ...]:
        with self._lock:
            leased: list[OutboxLease] = []
            for message_id in sorted(self._outbox):
                if len(leased) >= limit:
                    break
                state = self._outbox_state[message_id]
                if state["published_at"] is not None or state["next_attempt_at"] > now:
                    continue
                expires = state["lease_expires_at"]
                if expires is not None and expires > now:
                    continue
                record = self._outbox[message_id]
                token = uuid.uuid4().hex
                lease_expires_at = now + timedelta(seconds=lease_seconds)
                state.update(
                    lease_owner=relay_id,
                    lease_token=token,
                    lease_expires_at=lease_expires_at,
                    publish_attempts=state["publish_attempts"] + 1,
                )
                leased.append(
                    OutboxLease(
                        tenant_id=record.tenant_id,
                        job_id=record.job_id,
                        attempt_id=record.attempt_id,
                        channel=record.channel,
                        message_id=record.message_id,
                        content_digest=record.content_digest,
                        canonical_bytes=record.canonical_bytes,
                        sequence=record.sequence,
                        lease_token=token,
                        lease_expires_at=lease_expires_at,
                        publish_attempts=state["publish_attempts"],
                    )
                )
            return tuple(copy.deepcopy(leased))

    def renew_outbox_lease(
        self,
        message_id: str,
        lease_token: str,
        *,
        now: datetime,
        lease_seconds: int,
    ) -> bool:
        with self._lock:
            state = self._outbox_state.get(message_id)
            if (
                state is None
                or state["published_at"] is not None
                or state["lease_token"] != lease_token
                or state["lease_expires_at"] <= now
            ):
                return False
            state["lease_expires_at"] = now + timedelta(seconds=lease_seconds)
            return True

    def mark_outbox_published(
        self,
        message_id: str,
        lease_token: str,
        *,
        publication_id: str,
        published_at: datetime,
    ) -> bool:
        with self._lock:
            state = self._outbox_state.get(message_id)
            if state is None:
                return False
            if state["published_at"] is not None:
                return state["publication_id"] == publication_id
            if state["lease_token"] != lease_token:
                return False
            state.update(
                published_at=published_at,
                publication_id=publication_id,
                lease_owner=None,
                lease_token=None,
                lease_expires_at=None,
                last_error_code=None,
            )
            return True

    def release_outbox_lease(
        self,
        message_id: str,
        lease_token: str,
        *,
        next_attempt_at: datetime,
        error_code: str,
    ) -> bool:
        with self._lock:
            state = self._outbox_state.get(message_id)
            if (
                state is None
                or state["published_at"] is not None
                or state["lease_token"] != lease_token
            ):
                return False
            state.update(
                lease_owner=None,
                lease_token=None,
                lease_expires_at=None,
                next_attempt_at=next_attempt_at,
                last_error_code=error_code,
            )
            return True

    def dead_letter_digest(self, dead_letter_id: str) -> str | None:
        with self._lock:
            value = self._dead_letters.get(dead_letter_id)
            return value[0] if value else None

    def commit_dead_letter(
        self,
        job: Job,
        *,
        expected_version: int,
        record: DeadLetterRecord,
        disposition: str,
    ) -> Job:
        with self._lock:
            prior = self._dead_letters.get(record.dead_letter_id)
            record_digest = dead_letter_record_digest(record)
            if prior is not None:
                if prior[0] != record_digest:
                    raise SyntheticDataError(
                        ErrorCode.CONCURRENCY_CONFLICT, "dead-letter identity digest conflict"
                    )
                return self.get(job.tenant_id, job.job_id) or job
            if job.version != expected_version:
                self.save(job, expected_version=expected_version)
            self._dead_letters[record.dead_letter_id] = (record_digest, disposition)
            return copy.deepcopy(job)
