"""API-side SWP/v1 coordinator; the only component allowed to commit job metadata."""

from __future__ import annotations

import hashlib
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.jobs.models import Job, JobState
from juntai_synthetic_data.relay.models import DeadLetterRecord, dead_letter_record_digest
from juntai_synthetic_data.worker_protocol import (
    REQUIRED_CAPABILITIES,
    CancelEnvelope,
    DispatchEnvelope,
    ExactArtifactReference,
    ProtocolError,
    ResourceEnvelope,
    WorkerEventEnvelope,
    WorkloadIdentity,
)

from .artifacts import ExecutionInputPublisher

DISPATCH_CHANNEL = "synthetic.worker.dispatch.v1"
CONTROL_CHANNEL = "synthetic.worker.control.v1"
RESULT_CHANNEL = "synthetic.worker.result.v1"
DEAD_LETTER_CHANNEL = "synthetic.worker.dead-letter.v1"


@dataclass(frozen=True)
class OutboxRecord:
    tenant_id: str
    job_id: str
    attempt_id: str
    channel: str
    message_id: str
    content_digest: str
    canonical_bytes: bytes
    sequence: int


class ProtocolRepository(Protocol):
    def save_with_dispatch(
        self,
        job: Job,
        *,
        expected_version: int,
        input_artifact: ExactArtifactReference,
        outbox: OutboxRecord,
    ) -> Job: ...

    def save_with_control(
        self, job: Job, *, expected_version: int, outbox: OutboxRecord
    ) -> Job: ...

    def worker_event_digest(self, event_id: str) -> str | None: ...

    def worker_attempt_exists(self, tenant_id: str, job_id: str, attempt_id: str) -> bool: ...

    def commit_worker_event(
        self,
        job: Job,
        *,
        expected_version: int,
        event: WorkerEventEnvelope,
        disposition: str,
    ) -> Job: ...

    def dead_letter_digest(self, dead_letter_id: str) -> str | None: ...

    def commit_dead_letter(
        self,
        job: Job,
        *,
        expected_version: int,
        record: DeadLetterRecord,
        disposition: str,
    ) -> Job: ...


class ArtifactReferenceVerifier(Protocol):
    def verify(self, reference: ExactArtifactReference, *, tenant_id: str) -> None: ...


class StructuralArtifactVerifier:
    """Fail-closed structural verifier used until an injected Artifact SDK verifier is supplied."""

    def verify(self, reference: ExactArtifactReference, *, tenant_id: str) -> None:
        if reference.tenant_id != tenant_id:
            raise SyntheticDataError(
                ErrorCode.CONTRACT_INVALID,
                "Artifact tenant differs from job tenant",
                details={"protocol_error": "TENANT_MISMATCH"},
            )


class WorkerCoordinator:
    def __init__(
        self,
        *,
        repository: ProtocolRepository,
        inputs: ExecutionInputPublisher,
        source_revision: str,
        worker_image_digest: str,
        api_workload: WorkloadIdentity,
        executor_workload: WorkloadIdentity,
        verifier: ArtifactReferenceVerifier | None = None,
    ) -> None:
        self.repository = repository
        self.inputs = inputs
        self.source_revision = source_revision
        self.worker_image_digest = worker_image_digest
        self.api_workload = api_workload
        self.executor_workload = executor_workload
        self.verifier = verifier or StructuralArtifactVerifier()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(UTC)

    def queue(self, job: Job, *, expected_version: int, provider_version: str) -> Job:
        input_artifact = self.inputs.publish_input(job, source_revision=self.source_revision)
        attempt_number = job.active_attempt_number + 1
        attempt_id = f"attempt_{uuid.uuid4().hex}"
        message_id = f"dispatch_{uuid.uuid4().hex}"
        emitted_at = self._now()
        dispatch = DispatchEnvelope(
            messageId=message_id,
            tenantId=job.tenant_id,
            jobId=job.job_id,
            attemptId=attempt_id,
            attemptNumber=attempt_number,
            sequence=0,
            emittedAt=emitted_at,
            deadline=emitted_at
            + timedelta(seconds=job.request.provider.requirements.maximum_runtime_seconds),
            correlationId=job.job_id,
            producerWorkload=self.api_workload,
            requestDigest=job.request_digest,
            inputArtifact=input_artifact,
            providerId=job.provider_id or "unknown",
            providerVersion=provider_version,
            workerImageDigest=self.worker_image_digest,
            requiredCapabilities=REQUIRED_CAPABILITIES,
            minExecutorBinding="juntai.platform.synthetic-executor/v1",
            resourceEnvelope=ResourceEnvelope(
                cpuMillis=4000,
                memoryBytes=2_147_483_648,
                ephemeralBytes=max(job.request.generation_contract.bounds.max_bytes * 2, 1_048_576),
                processLimit=128,
            ),
            idempotencyKeyDigest=(
                "sha256:" + hashlib.sha256(job.idempotency_key.encode()).hexdigest()
            ),
        ).signed()
        job.active_attempt_id = attempt_id
        job.active_attempt_number = attempt_number
        outbox = OutboxRecord(
            tenant_id=job.tenant_id,
            job_id=job.job_id,
            attempt_id=attempt_id,
            channel=DISPATCH_CHANNEL,
            message_id=message_id,
            content_digest=dispatch.content_digest or "",
            canonical_bytes=dispatch.canonical_bytes(),
            sequence=0,
        )
        return self.repository.save_with_dispatch(
            job,
            expected_version=expected_version,
            input_artifact=input_artifact,
            outbox=outbox,
        )

    def cancel(self, job: Job, *, expected_version: int) -> Job:
        if job.active_attempt_id is None:
            raise SyntheticDataError(
                ErrorCode.CONCURRENCY_CONFLICT, "queued job has no active SWP attempt"
            )
        job.cancel_sequence += 1
        emitted_at = self._now()
        message = CancelEnvelope(
            messageId=f"cancel_{uuid.uuid4().hex}",
            tenantId=job.tenant_id,
            jobId=job.job_id,
            attemptId=job.active_attempt_id,
            attemptNumber=job.active_attempt_number,
            sequence=job.cancel_sequence,
            emittedAt=emitted_at,
            deadline=emitted_at + timedelta(seconds=60),
            correlationId=job.job_id,
            producerWorkload=self.api_workload,
            cancelSequence=job.cancel_sequence,
            requestedAt=emitted_at,
            reasonCode="caller-requested",
            requestedByKind="delegated",
            graceDeadline=emitted_at + timedelta(seconds=30),
        ).signed()
        outbox = OutboxRecord(
            tenant_id=job.tenant_id,
            job_id=job.job_id,
            attempt_id=job.active_attempt_id,
            channel=CONTROL_CHANNEL,
            message_id=message.message_id,
            content_digest=message.content_digest or "",
            canonical_bytes=message.canonical_bytes(),
            sequence=job.cancel_sequence,
        )
        return self.repository.save_with_control(
            job, expected_version=expected_version, outbox=outbox
        )

    def accept_event(
        self,
        job: Job,
        event: WorkerEventEnvelope,
        *,
        authenticated_producer: WorkloadIdentity,
    ) -> str:
        try:
            event.verify(authenticated_producer=authenticated_producer)
        except ProtocolError as error:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "result producer identity or envelope integrity failed",
                details={"protocol_error": error.code},
            ) from error
        if authenticated_producer != self.executor_workload:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "result producer is not the pinned Platform executor",
                details={"protocol_error": "IDENTITY_MISMATCH"},
            )
        if event.tenant_id != job.tenant_id:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "result tenant differs from job tenant",
                details={"protocol_error": "TENANT_MISMATCH"},
            )
        prior_digest = self.repository.worker_event_digest(event.event_id)
        if prior_digest is not None:
            if prior_digest != event.content_digest:
                raise SyntheticDataError(
                    ErrorCode.CONCURRENCY_CONFLICT,
                    "event identity was reused with different content",
                    details={"protocol_error": "RESULT_CONFLICT"},
                )
            return "RESULT_DUPLICATE"
        if event.attempt_id != job.active_attempt_id or job.terminal:
            if not self.repository.worker_attempt_exists(
                event.tenant_id, event.job_id, event.attempt_id
            ):
                raise SyntheticDataError(
                    ErrorCode.CONTRACT_INVALID,
                    "worker event names an unknown SWP attempt",
                    details={"protocol_error": "ATTEMPT_STALE"},
                )
            self.repository.commit_worker_event(
                job,
                expected_version=job.version,
                event=event,
                disposition="ATTEMPT_STALE",
            )
            return "ATTEMPT_STALE"
        if event.worker_image_digest != job.worker_image_digest:
            raise SyntheticDataError(
                ErrorCode.CONTRACT_INVALID,
                "worker event image does not match the admitted immutable image",
                details={"protocol_error": "IDENTITY_MISMATCH"},
            )
        if not set(REQUIRED_CAPABILITIES).issubset(event.protocol_capabilities):
            raise SyntheticDataError(
                ErrorCode.CONTRACT_INVALID,
                "worker event lacks required SWP/v1 capabilities",
                details={"protocol_error": "PROTOCOL_UNSUPPORTED"},
            )
        if event.event_type == "TERMINAL":
            if event.consumed_input_digest != job.request_digest:
                raise SyntheticDataError(
                    ErrorCode.CONTRACT_INVALID,
                    "worker consumed a different immutable input",
                    details={"protocol_error": "ARTIFACT_INTEGRITY_FAILED"},
                )
            if event.execution_evidence_artifact is None:
                raise SyntheticDataError(
                    ErrorCode.CONTRACT_INVALID,
                    "terminal event lacks immutable execution evidence",
                )
            self.verifier.verify(event.execution_evidence_artifact, tenant_id=job.tenant_id)
        expected = job.version
        if event.event_type == "STARTED":
            if job.state is JobState.QUEUED:
                job.transition(JobState.RUNNING)
        elif event.event_type == "STAGE":
            target = JobState(event.stage)
            if job.state is not JobState.CANCELLING and target is not job.state:
                job.transition(target)
        elif event.outcome == "SUCCEEDED":
            if job.state is JobState.CANCELLING:
                job.transition(JobState.CANCELLED, reason="cancellation committed before success")
            else:
                if event.dataset_artifact is None or event.execution_evidence_artifact is None:
                    raise SyntheticDataError(
                        ErrorCode.CONTRACT_INVALID, "successful event lacks Artifacts"
                    )
                self.verifier.verify(event.dataset_artifact, tenant_id=job.tenant_id)
                if job.state is not JobState.PUBLISHING:
                    job.transition(JobState.PUBLISHING)
                job.result = {
                    "job_id": job.job_id,
                    "artifact": {
                        "artifact_id": event.dataset_artifact.artifact_id,
                        "version_id": event.dataset_artifact.version_id,
                        "digest": event.dataset_artifact.manifest_digest,
                        "media_type": event.dataset_artifact.media_type,
                    },
                    "manifest_digest": event.dataset_artifact.manifest_digest,
                    "format": job.request.generation_contract.output.format,
                    "compression": job.request.generation_contract.output.compression,
                    "record_count": event.output_records or 0,
                    "byte_count": event.output_bytes or 0,
                    "seed": job.request.seed,
                    "validator_passed": None,
                    "provenance": {
                        "schema_version": "juntai.synthetic-data.provenance/v1",
                        "job_id": job.job_id,
                        "request_digest": job.request_digest,
                        "contract_digest": job.request.generation_contract.digest,
                        "provider_id": job.provider_id or "unknown",
                        "provider_version": "worker-event",
                        "seed": job.request.seed,
                        "policy_digest": str(
                            (job.quota or {}).get("policy_digest", "sha256:" + "0" * 64)
                        ),
                        "quota_reservation_id": str(
                            (job.quota or {}).get("reservation_id", "unknown")
                        ),
                        "worker_image_digest": event.worker_image_digest,
                        "logical_dataset_digest": event.dataset_artifact.manifest_digest,
                        "artifact_digest": event.dataset_artifact.manifest_digest,
                        "record_count": event.output_records or 0,
                        "byte_count": event.output_bytes or 0,
                        "shard_count": int(event.evidence_counters.get("shards", 0)),
                        "started_at": event.started_at,
                        "completed_at": event.terminal_at,
                    },
                }
                job.transition(JobState.SUCCEEDED)
        elif event.event_type == "TERMINAL":
            if event.outcome == "CANCELLED" or job.state is JobState.CANCELLING:
                if job.state is not JobState.CANCELLING:
                    job.request_cancellation()
                job.transition(JobState.CANCELLED, reason="CANCELLED")
            else:
                code = event.error.code if event.error else "DEPENDENCY_UNAVAILABLE"
                job.fail(
                    SyntheticDataError(
                        ErrorCode.DEPENDENCY_UNAVAILABLE,
                        event.error.message if event.error else "worker execution failed",
                        retryable=bool(event.error and event.error.retryable),
                        details={"protocol_error": code},
                    )
                )
        disposition = "COMMITTED"
        self.repository.commit_worker_event(
            job, expected_version=expected, event=event, disposition=disposition
        )
        return disposition

    def accept_dead_letter(self, job: Job, record: DeadLetterRecord) -> str:
        record_digest = dead_letter_record_digest(record)
        prior_digest = self.repository.dead_letter_digest(record.dead_letter_id)
        if prior_digest is not None:
            if prior_digest != record_digest:
                raise SyntheticDataError(
                    ErrorCode.CONCURRENCY_CONFLICT,
                    "dead-letter identity was reused with different content",
                    details={"protocol_error": "RESULT_CONFLICT"},
                )
            return "DEAD_LETTER_DUPLICATE"
        expected_producer = (
            self.executor_workload
            if record.original_channel == RESULT_CHANNEL
            else self.api_workload
        )
        if record.authenticated_producer != expected_producer:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "dead-letter producer is not the pinned workload",
                details={"protocol_error": "IDENTITY_MISMATCH"},
            )
        if record.tenant_id != job.tenant_id:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "dead-letter tenant differs from job tenant",
                details={"protocol_error": "TENANT_MISMATCH"},
            )
        expected = job.version
        if record.delivery_count != 5:
            raise SyntheticDataError(
                ErrorCode.CONTRACT_INVALID,
                "dead-letter evidence has an invalid delivery count",
                details={"protocol_error": "ENVELOPE_INVALID"},
            )
        if record.attempt_id != job.active_attempt_id or job.terminal:
            disposition = "ATTEMPT_STALE"
        elif record.original_channel == RESULT_CHANNEL and record.event_id is not None:
            result_digest = self.repository.worker_event_digest(record.event_id)
            if result_digest is not None:
                disposition = (
                    "RESULT_DUPLICATE"
                    if result_digest == record.content_digest
                    else "RESULT_CONFLICT"
                )
            else:
                job.fail(
                    SyntheticDataError(
                        ErrorCode.DELIVERY_EXHAUSTED,
                        "SWP message exhausted its five-delivery budget",
                        details={
                            "protocol_error": "DELIVERY_EXHAUSTED",
                            "channel": record.original_channel,
                        },
                    )
                )
                disposition = "DELIVERY_EXHAUSTED"
        elif record.original_channel == CONTROL_CHANNEL:
            disposition = "RECONCILE_CANCELLING"
        else:
            job.fail(
                SyntheticDataError(
                    ErrorCode.DELIVERY_EXHAUSTED,
                    "SWP message exhausted its five-delivery budget",
                    details={
                        "protocol_error": "DELIVERY_EXHAUSTED",
                        "channel": record.original_channel,
                    },
                )
            )
            disposition = "DELIVERY_EXHAUSTED"
        self.repository.commit_dead_letter(
            job,
            expected_version=expected,
            record=record,
            disposition=disposition,
        )
        return disposition
