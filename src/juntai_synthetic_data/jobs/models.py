"""Accepted-to-terminal job state machine."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


def now_utc() -> datetime:
    return datetime.now(UTC)


class JobState(StrEnum):
    ACCEPTED = "ACCEPTED"
    POLICY_CHECK = "POLICY_CHECK"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    VALIDATING = "VALIDATING"
    PUBLISHING = "PUBLISHING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLING = "CANCELLING"
    CANCELLED = "CANCELLED"


TERMINAL_STATES = frozenset({JobState.SUCCEEDED, JobState.FAILED, JobState.CANCELLED})
ALLOWED_TRANSITIONS: dict[JobState, frozenset[JobState]] = {
    JobState.ACCEPTED: frozenset({JobState.POLICY_CHECK, JobState.CANCELLING, JobState.FAILED}),
    JobState.POLICY_CHECK: frozenset({JobState.QUEUED, JobState.CANCELLING, JobState.FAILED}),
    JobState.QUEUED: frozenset({JobState.RUNNING, JobState.CANCELLING, JobState.FAILED}),
    JobState.RUNNING: frozenset(
        {JobState.VALIDATING, JobState.PUBLISHING, JobState.CANCELLING, JobState.FAILED}
    ),
    JobState.VALIDATING: frozenset({JobState.PUBLISHING, JobState.CANCELLING, JobState.FAILED}),
    JobState.PUBLISHING: frozenset({JobState.SUCCEEDED, JobState.FAILED}),
    JobState.CANCELLING: frozenset({JobState.CANCELLED, JobState.FAILED}),
    JobState.SUCCEEDED: frozenset(),
    JobState.FAILED: frozenset(),
    JobState.CANCELLED: frozenset(),
}


@dataclass(frozen=True)
class Transition:
    sequence: int
    from_state: JobState | None
    to_state: JobState
    occurred_at: datetime
    reason: str | None = None


@dataclass
class Job:
    job_id: str
    tenant_id: str
    idempotency_key: str
    request_digest: str
    request: CreateJobRequest
    state: JobState = JobState.ACCEPTED
    version: int = 0
    created_at: datetime = field(default_factory=now_utc)
    updated_at: datetime = field(default_factory=now_utc)
    transitions: list[Transition] = field(default_factory=list)
    quota: dict[str, Any] | None = None
    provider_id: str | None = None
    worker_image_digest: str | None = None
    failure: dict[str, Any] | None = None
    result: dict[str, Any] | None = None
    cancellation_requested: bool = False

    def __post_init__(self) -> None:
        if not self.transitions:
            self.transitions.append(
                Transition(0, None, JobState.ACCEPTED, self.created_at, "request accepted")
            )

    @property
    def terminal(self) -> bool:
        return self.state in TERMINAL_STATES

    def transition(self, target: JobState, *, reason: str | None = None) -> None:
        if self.terminal:
            raise SyntheticDataError(ErrorCode.CONCURRENCY_CONFLICT, "terminal job is immutable")
        if target not in ALLOWED_TRANSITIONS[self.state]:
            raise SyntheticDataError(
                ErrorCode.CONCURRENCY_CONFLICT,
                f"invalid job transition {self.state.value} -> {target.value}",
            )
        previous = self.state
        self.state = target
        self.version += 1
        self.updated_at = now_utc()
        self.transitions.append(
            Transition(len(self.transitions), previous, target, self.updated_at, reason)
        )

    def request_cancellation(self) -> None:
        if self.terminal or self.state is JobState.PUBLISHING:
            return
        self.cancellation_requested = True
        if self.state is not JobState.CANCELLING:
            self.transition(JobState.CANCELLING, reason="cancellation requested")

    def fail(self, error: SyntheticDataError) -> None:
        if self.terminal:
            return
        self.failure = error.to_dict()
        self.transition(JobState.FAILED, reason=error.code.value)
