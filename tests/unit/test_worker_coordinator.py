from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

import pytest
from conftest import IMAGE_DIGEST, FakePublisher, request_data

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.errors import SyntheticDataError
from juntai_synthetic_data.execution import WorkerCoordinator
from juntai_synthetic_data.jobs import InMemoryJobRepository, JobState
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.providers import DeterministicTabularProvider, ProviderRegistry
from juntai_synthetic_data.quotas import InMemoryQuotaLedger, QuotaLimits
from juntai_synthetic_data.service import SyntheticDataService
from juntai_synthetic_data.worker_protocol import (
    EVIDENCE_MEDIA_TYPE,
    INPUT_MEDIA_TYPE,
    DispatchEnvelope,
    ExactArtifactReference,
    ProgressBounds,
    WorkerEventEnvelope,
    WorkloadIdentity,
    decode_envelope,
)

API = WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-api")
EXECUTOR = WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-executor")
WORKER = WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-worker")


@dataclass
class Inputs:
    def publish_input(self, job, *, source_revision: str) -> ExactArtifactReference:
        return ExactArtifactReference(
            tenantId=job.tenant_id,
            artifactId=f"art-{job.job_id}",
            versionId="artv-input-1",
            manifestDigest="sha256:" + "5" * 64,
            mediaType=INPUT_MEDIA_TYPE,
            byteLength=4096,
            producerBuildId=source_revision,
        )


def make_coordinated_service():
    repository = InMemoryJobRepository()
    coordinator = WorkerCoordinator(
        repository=repository,
        inputs=Inputs(),
        source_revision="a" * 40,
        worker_image_digest=IMAGE_DIGEST,
        api_workload=API,
        executor_workload=EXECUTOR,
    )
    service = SyntheticDataService(
        repository=repository,
        providers=ProviderRegistry(
            (DeterministicTabularProvider(worker_image_digest=IMAGE_DIGEST),)
        ),
        policy=DefaultPolicyEngine(),
        quotas=InMemoryQuotaLedger(QuotaLimits()),
        publisher=FakePublisher(),
        source_revision="a" * 40,
        coordinator=coordinator,
    )
    return service, repository


def artifact(media_type: str, digit: str) -> ExactArtifactReference:
    return ExactArtifactReference(
        tenantId="tenant-a",
        artifactId=f"art-{digit}",
        versionId=f"artv-{digit}",
        manifestDigest="sha256:" + digit * 64,
        mediaType=media_type,
        byteLength=128,
        producerBuildId="a" * 40,
    )


def event(
    dispatch: DispatchEnvelope,
    *,
    event_id: str,
    event_type: str,
    stage: str,
    sequence: int,
    outcome: str | None = None,
) -> WorkerEventEnvelope:
    now = datetime.now(UTC)
    terminal = event_type == "TERMINAL"
    return WorkerEventEnvelope(
        messageId=f"message-{event_id}-{sequence}",
        tenantId=dispatch.tenant_id,
        jobId=dispatch.job_id,
        attemptId=dispatch.attempt_id,
        attemptNumber=dispatch.attempt_number,
        sequence=sequence,
        emittedAt=now,
        deadline=now + timedelta(minutes=5),
        correlationId=dispatch.correlation_id,
        producerWorkload=EXECUTOR,
        eventId=event_id,
        eventType=event_type,
        executionLeaseId="lease-1",
        stage=stage,
        progressBounds=ProgressBounds(completed=1, total=1),
        observedCancelSequence=0,
        workerImageDigest=dispatch.worker_image_digest,
        protocolCapabilities=dispatch.required_capabilities,
        evidenceCounters={"events": sequence + 1, "shards": 1},
        outcome=outcome,
        datasetArtifact=artifact("application/vnd.oci.image.manifest.v1+json", "6")
        if outcome == "SUCCEEDED"
        else None,
        executionEvidenceArtifact=artifact(EVIDENCE_MEDIA_TYPE, "7") if terminal else None,
        startedAt=now if terminal else None,
        terminalAt=now if terminal else None,
        outputRecords=1 if outcome == "SUCCEEDED" else None,
        outputBytes=128 if outcome == "SUCCEEDED" else None,
        consumedInputDigest=dispatch.request_digest if terminal else None,
    ).signed()


def queued():
    service, repository = make_coordinated_service()
    request = CreateJobRequest.model_validate(request_data())
    status = service.create_job("tenant-a", "coordinator-key", request)
    dispatch = decode_envelope(repository.pending_outbox()[0].canonical_bytes)
    assert isinstance(dispatch, DispatchEnvelope)
    return service, repository, status, dispatch


def test_create_atomically_records_attempt_and_canonical_dispatch_outbox() -> None:
    service, repository, status, dispatch = queued()
    assert status.state == JobState.QUEUED
    assert dispatch.request_digest == service.get_job("tenant-a", status.job_id).request_digest
    assert dispatch.producer_workload == API
    assert repository.pending_outbox()[0].channel == "synthetic.worker.dispatch.v1"


def test_start_stage_success_commit_and_result_replay_are_idempotent() -> None:
    service, _, status, dispatch = queued()
    started = event(
        dispatch, event_id="event-started", event_type="STARTED", stage="RUNNING", sequence=0
    )
    assert service.accept_worker_event(started, authenticated_producer=EXECUTOR) == "COMMITTED"
    publishing = event(
        dispatch,
        event_id="event-publishing",
        event_type="STAGE",
        stage="PUBLISHING",
        sequence=1,
    )
    service.accept_worker_event(publishing, authenticated_producer=EXECUTOR)
    success = event(
        dispatch,
        event_id="event-terminal",
        event_type="TERMINAL",
        stage="PUBLISHING",
        sequence=2,
        outcome="SUCCEEDED",
    )
    assert service.accept_worker_event(success, authenticated_producer=EXECUTOR) == "COMMITTED"
    assert service.get_job("tenant-a", status.job_id).state is JobState.SUCCEEDED
    assert (
        service.accept_worker_event(success, authenticated_producer=EXECUTOR) == "RESULT_DUPLICATE"
    )
    conflicting = success.model_copy(update={"sequence": 3, "content_digest": None}).signed()
    with pytest.raises(SyntheticDataError, match="different content"):
        service.accept_worker_event(conflicting, authenticated_producer=EXECUTOR)


def test_cancellation_outbox_and_kes_commit_order_prevent_late_success_association() -> None:
    service, repository, status, dispatch = queued()
    cancelled = service.cancel("tenant-a", status.job_id)
    assert cancelled.state == JobState.CANCELLING
    assert [item.channel for item in repository.pending_outbox()] == [
        "synthetic.worker.control.v1",
        "synthetic.worker.dispatch.v1",
    ]
    success = event(
        dispatch,
        event_id="event-late-success",
        event_type="TERMINAL",
        stage="PUBLISHING",
        sequence=1,
        outcome="SUCCEEDED",
    )
    service.accept_worker_event(success, authenticated_producer=EXECUTOR)
    job = service.get_job("tenant-a", status.job_id)
    assert job.state is JobState.CANCELLED
    assert job.result is None


def test_result_identity_and_tenant_spoofing_fail_closed() -> None:
    service, _, _, dispatch = queued()
    started = event(
        dispatch, event_id="event-spoof", event_type="STARTED", stage="RUNNING", sequence=0
    )
    with pytest.raises(SyntheticDataError, match="identity"):
        service.accept_worker_event(started, authenticated_producer=WORKER)
    spoofed = started.model_copy(update={"tenant_id": "tenant-b", "content_digest": None}).signed()
    with pytest.raises(SyntheticDataError):
        service.accept_worker_event(spoofed, authenticated_producer=EXECUTOR)


def test_terminal_input_image_and_capability_integrity_fail_closed() -> None:
    service, _, _, dispatch = queued()
    terminal = event(
        dispatch,
        event_id="event-integrity",
        event_type="TERMINAL",
        stage="PUBLISHING",
        sequence=0,
        outcome="SUCCEEDED",
    )
    wrong_image = terminal.model_copy(
        update={"worker_image_digest": "sha256:" + "8" * 64, "content_digest": None}
    ).signed()
    with pytest.raises(SyntheticDataError, match="immutable image"):
        service.accept_worker_event(wrong_image, authenticated_producer=EXECUTOR)
    wrong_input = terminal.model_copy(
        update={"consumed_input_digest": "sha256:" + "9" * 64, "content_digest": None}
    ).signed()
    with pytest.raises(SyntheticDataError, match="different immutable input"):
        service.accept_worker_event(wrong_input, authenticated_producer=EXECUTOR)
    missing_capability = terminal.model_copy(
        update={"protocol_capabilities": (), "content_digest": None}
    ).signed()
    with pytest.raises(SyntheticDataError, match="lacks required"):
        service.accept_worker_event(missing_capability, authenticated_producer=EXECUTOR)
