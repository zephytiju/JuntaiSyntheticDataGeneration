from __future__ import annotations

import pytest
from conftest import make_service, request_data

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.errors import ErrorCode
from juntai_synthetic_data.jobs import JobState
from juntai_synthetic_data.quotas import QuotaLimits


def test_source_examples_require_explicit_authorization() -> None:
    data = request_data()
    data["policy"]["source_examples"] = "minimized"
    service = make_service()
    status = service.create_job("tenant-a", "source-key", CreateJobRequest.model_validate(data))
    assert status.state == JobState.FAILED
    assert status.failure and status.failure.code == ErrorCode.POLICY_DENIED


@pytest.mark.parametrize("classification", ["confidential", "restricted"])
def test_sensitive_classification_fails_closed(classification: str) -> None:
    data = request_data()
    data["policy"]["data_classification"] = classification
    status = make_service().create_job(
        "tenant-a", f"policy-{classification}", CreateJobRequest.model_validate(data)
    )
    assert status.failure and status.failure.code == ErrorCode.POLICY_DENIED


def test_concurrency_quota_reservation_and_release() -> None:
    service = make_service(limits=QuotaLimits(concurrent_jobs=1))
    request = CreateJobRequest.model_validate(request_data())
    first = service.create_job("tenant-a", "quota-1", request)
    second = service.create_job("tenant-a", "quota-2", request)
    assert first.state == JobState.QUEUED
    assert second.state == JobState.FAILED
    assert second.failure and second.failure.code == ErrorCode.QUOTA_EXCEEDED
    service.cancel("tenant-a", first.job_id)
    third = service.create_job("tenant-a", "quota-3", request)
    assert third.state == JobState.QUEUED
