from __future__ import annotations

from conftest import (
    ARTIFACT_DIGEST,
    IMAGE_DIGEST,
    FakePublisher,
    PassingExecutor,
    make_service,
    request_data,
)
from juntai.usage import UsageReporter

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.errors import ErrorCode
from juntai_synthetic_data.jobs import JobState


def test_accepted_job_reaches_success_with_complete_provenance() -> None:
    publisher = FakePublisher()
    service = make_service(publisher=publisher)
    request = CreateJobRequest.model_validate(request_data())
    created = service.create_job("tenant-a", "acceptance-1", request)
    completed = service.run_job("tenant-a", created.job_id)
    result = service.result("tenant-a", created.job_id)
    assert completed.state == JobState.SUCCEEDED
    assert result.artifact.digest == ARTIFACT_DIGEST
    assert result.record_count == 9
    assert result.provenance.request_digest == request.digest
    assert result.provenance.contract_digest == request.generation_contract.digest
    assert result.provenance.worker_image_digest == IMAGE_DIGEST
    assert result.provenance.artifact_digest == ARTIFACT_DIGEST
    assert result.provenance.logical_dataset_digest == result.manifest_digest
    assert service.quotas.active_count == 0
    assert len(publisher.calls) == 1


def test_validator_job_records_exact_reference_and_evidence() -> None:
    executor = PassingExecutor()
    service = make_service(executor=executor)
    request = CreateJobRequest.model_validate(request_data(validator=True))
    created = service.create_job("tenant-a", "acceptance-validator", request)
    completed = service.run_job("tenant-a", created.job_id)
    result = service.result("tenant-a", created.job_id)
    assert completed.state == JobState.SUCCEEDED
    assert result.validator_passed is True
    assert result.provenance.validator_reference
    assert result.provenance.validation_digest
    assert executor.requests[0].descriptor.digest == request.validator.digest  # type: ignore[union-attr]


def test_missing_validator_sandbox_fails_without_publication() -> None:
    publisher = FakePublisher()
    service = make_service(publisher=publisher)
    request = CreateJobRequest.model_validate(request_data(validator=True))
    created = service.create_job("tenant-a", "acceptance-no-sandbox", request)
    completed = service.run_job("tenant-a", created.job_id)
    assert completed.state == JobState.FAILED
    assert completed.failure and completed.failure.code == ErrorCode.DEPENDENCY_UNAVAILABLE
    assert not publisher.calls


def test_publication_failure_is_terminal_without_result() -> None:
    service = make_service(publisher=FakePublisher(fail=True))
    request = CreateJobRequest.model_validate(request_data())
    created = service.create_job("tenant-a", "acceptance-publish-fail", request)
    completed = service.run_job("tenant-a", created.job_id)
    assert completed.state == JobState.FAILED
    assert completed.failure and completed.failure.code == ErrorCode.PUBLICATION_FAILED
    assert service.quotas.active_count == 0


def test_cancellation_after_commit_does_not_mutate_result() -> None:
    service = make_service()
    request = CreateJobRequest.model_validate(request_data())
    created = service.create_job("tenant-a", "acceptance-commit", request)
    service.run_job("tenant-a", created.job_id)
    before = service.result("tenant-a", created.job_id)
    status = service.cancel("tenant-a", created.job_id)
    after = service.result("tenant-a", created.job_id)
    assert status.state == JobState.SUCCEEDED
    assert before == after


def test_success_reports_canonical_record_and_byte_usage() -> None:
    emitted = []
    reporter = UsageReporter(
        emitted.append,
        resource_attributes={
            "service.namespace": "juntai",
            "service.name": "synthetic-data-generation",
        },
    )
    service = make_service(usage_reporter=reporter)
    request = CreateJobRequest.model_validate(request_data())
    created = service.create_job("tenant-a", "acceptance-usage", request)
    service.run_job("tenant-a", created.job_id)
    assert [event.attributes["juntai.usage.meter"] for event in emitted] == [
        "synthetic_data.records",
        "synthetic_data.bytes",
    ]
