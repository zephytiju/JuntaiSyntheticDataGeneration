from __future__ import annotations

import pytest
from conftest import make_service, request_data

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.jobs import Job, JobState


def test_terminal_job_is_immutable(sample_request: CreateJobRequest) -> None:
    job = Job("job_test", "tenant", "key", sample_request.digest, sample_request)
    job.transition(JobState.POLICY_CHECK)
    job.transition(JobState.QUEUED)
    job.transition(JobState.RUNNING)
    job.transition(JobState.PUBLISHING)
    job.transition(JobState.SUCCEEDED)
    with pytest.raises(SyntheticDataError, match="terminal job is immutable"):
        job.transition(JobState.FAILED)


def test_invalid_transition_is_rejected(sample_request: CreateJobRequest) -> None:
    job = Job("job_test", "tenant", "key", sample_request.digest, sample_request)
    with pytest.raises(SyntheticDataError, match="invalid job transition"):
        job.transition(JobState.SUCCEEDED)


def test_same_idempotency_key_and_request_returns_same_job() -> None:
    service = make_service()
    request = CreateJobRequest.model_validate(request_data())
    first = service.create_job("tenant-a", "same-key", request)
    second = service.create_job("tenant-a", "same-key", request)
    assert first.job_id == second.job_id


def test_changed_request_with_same_idempotency_key_fails() -> None:
    service = make_service()
    first = CreateJobRequest.model_validate(request_data())
    changed_data = request_data()
    changed_data["seed"] = "changed"
    changed = CreateJobRequest.model_validate(changed_data)
    service.create_job("tenant-a", "same-key", first)
    with pytest.raises(SyntheticDataError) as captured:
        service.create_job("tenant-a", "same-key", changed)
    assert captured.value.code is ErrorCode.IDEMPOTENCY_KEY_REUSED


def test_queued_cancellation_is_terminal_and_idempotent() -> None:
    service = make_service()
    request = CreateJobRequest.model_validate(request_data())
    created = service.create_job("tenant-a", "cancel-key", request)
    cancelled = service.cancel("tenant-a", created.job_id)
    repeated = service.cancel("tenant-a", created.job_id)
    assert cancelled.state == repeated.state == JobState.CANCELLED
    assert service.quotas.active_count == 0
