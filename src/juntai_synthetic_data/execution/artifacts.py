"""Exact immutable Artifact inputs and execution evidence for SWP/v1."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any, Protocol

import grpc
from juntai.artifact import ArtifactClient, ArtifactLayer, Provenance
from juntai.artifact.errors import ArtifactError

from juntai_synthetic_data.contracts.models import CreateJobRequest, canonical_json
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.jobs.models import Job
from juntai_synthetic_data.worker_protocol import (
    EVIDENCE_MEDIA_TYPE,
    INPUT_MEDIA_TYPE,
    ExactArtifactReference,
)


class ExecutionInputPublisher(Protocol):
    def publish_input(self, job: Job, *, source_revision: str) -> ExactArtifactReference: ...


class ExecutionInputResolver(Protocol):
    def resolve_input(self, reference: ExactArtifactReference) -> CreateJobRequest: ...


class ExecutionEvidencePublisher(Protocol):
    def publish_evidence(
        self,
        *,
        tenant_id: str,
        job_id: str,
        attempt_id: str,
        source_revision: str,
        evidence: dict[str, Any],
    ) -> ExactArtifactReference: ...


class ArtifactExactReferenceVerifier:
    """Resolve exact Artifact coordinates before the API commits a worker result."""

    def __init__(self, client: ArtifactClient, caller_credentials: grpc.CallCredentials) -> None:
        self.client = client
        self.caller_credentials = caller_credentials

    def verify(self, reference: ExactArtifactReference, *, tenant_id: str) -> None:
        if reference.tenant_id != tenant_id:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "Artifact tenant differs from authenticated job tenant",
                details={"protocol_error": "TENANT_MISMATCH"},
            )
        try:
            metadata = self.client.resolve_metadata(
                artifact_id=reference.artifact_id,
                version_id=reference.version_id,
                digest=reference.manifest_digest,
                caller_credentials=self.caller_credentials,
                correlation_id=reference.artifact_id,
            )
            if metadata.tenant_id != tenant_id:
                raise ValueError("Artifact Registry tenant mismatch")
            if reference.media_type not in {
                metadata.manifest_media_type,
                *metadata.layer_media_types,
            }:
                raise ValueError("Artifact media type does not match exact Registry metadata")
            if metadata.provenance.build_id != reference.producer_build_id:
                raise ValueError("Artifact producer build does not match exact Registry metadata")
        except (ArtifactError, TimeoutError, ConnectionError, ValueError) as error:
            raise SyntheticDataError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "exact result Artifact verification failed",
                retryable=False,
                details={"protocol_error": "ARTIFACT_INTEGRITY_FAILED"},
            ) from error


def _created_at() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class ArtifactExecutionInputPublisher:
    def __init__(self, client: ArtifactClient, *, producer_build_id: str) -> None:
        self.client = client
        self.producer_build_id = producer_build_id

    def publish_input(self, job: Job, *, source_revision: str) -> ExactArtifactReference:
        payload = canonical_json(
            {
                "schemaVersion": "juntai.synthetic.worker-input/v1",
                "tenantId": job.tenant_id,
                "jobId": job.job_id,
                "requestDigest": job.request_digest,
                "request": job.request.model_dump(mode="json", by_alias=True, exclude_none=True),
            }
        )
        try:
            published = self.client.publish(
                namespace="synthetic-data",
                name=f"worker-input-{job.job_id.removeprefix('job_')[:24]}",
                kind="synthetic-data.worker-input",
                version=f"job-{job.job_id.removeprefix('job_')[:32]}",
                layers=(ArtifactLayer(media_type=INPUT_MEDIA_TYPE, data=payload),),
                provenance=Provenance(
                    producer_identity="juntai-synthetic-data-api",
                    source_revision=source_revision,
                    build_id=self.producer_build_id,
                    created_at=_created_at(),
                ),
                idempotency_key=f"synthetic-worker-input:{job.request_digest}",
                labels={"protocol": "juntai.synthetic.worker/v1"},
                annotations={"request-digest": job.request_digest},
                correlation_id=job.job_id,
            )
        except (ArtifactError, TimeoutError, ConnectionError, ValueError) as error:
            raise SyntheticDataError(
                ErrorCode.PUBLICATION_FAILED,
                "worker input Artifact publication failed",
                retryable=True,
            ) from error
        reference = published.reference
        return ExactArtifactReference(
            tenantId=job.tenant_id,
            artifactId=reference.artifact_id,
            versionId=reference.version_id,
            manifestDigest=reference.manifest_digest,
            mediaType=INPUT_MEDIA_TYPE,
            byteLength=len(payload),
            producerBuildId=self.producer_build_id,
        )


class ArtifactExecutionInputResolver:
    def __init__(self, client: ArtifactClient, caller_credentials: grpc.CallCredentials) -> None:
        self.client = client
        self.caller_credentials = caller_credentials

    def resolve_input(self, reference: ExactArtifactReference) -> CreateJobRequest:
        if reference.media_type != INPUT_MEDIA_TYPE:
            raise SyntheticDataError(
                ErrorCode.CONTRACT_INVALID, "worker input media type is invalid"
            )
        try:
            metadata = self.client.resolve_metadata(
                artifact_id=reference.artifact_id,
                version_id=reference.version_id,
                digest=reference.manifest_digest,
                caller_credentials=self.caller_credentials,
            )
            if metadata.manifest_digest != reference.manifest_digest:
                raise ValueError("worker input manifest digest mismatch")
            layers = tuple(self.client.resolve_and_download(metadata))
            if len(layers) != 1 or layers[0].media_type != INPUT_MEDIA_TYPE:
                raise ValueError("worker input must contain one exact protocol layer")
            if len(layers[0].data) != reference.byte_length:
                raise ValueError("worker input byte length mismatch")
            document = json.loads(layers[0].data)
            if document.get("tenantId") != reference.tenant_id:
                raise ValueError("worker input tenant mismatch")
            request = CreateJobRequest.model_validate(document["request"])
            if document.get("requestDigest") != request.digest:
                raise ValueError("worker input request digest mismatch")
            return request
        except (
            ArtifactError,
            TimeoutError,
            ConnectionError,
            ValueError,
            KeyError,
            json.JSONDecodeError,
        ) as error:
            raise SyntheticDataError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "exact worker input Artifact resolution failed",
                retryable=False,
                details={"protocol_error": "ARTIFACT_INTEGRITY_FAILED"},
            ) from error


class ArtifactExecutionEvidencePublisher:
    def __init__(self, client: ArtifactClient, *, producer_build_id: str) -> None:
        self.client = client
        self.producer_build_id = producer_build_id

    def publish_evidence(
        self,
        *,
        tenant_id: str,
        job_id: str,
        attempt_id: str,
        source_revision: str,
        evidence: dict[str, Any],
    ) -> ExactArtifactReference:
        payload = canonical_json(
            {"schemaVersion": "juntai.synthetic.execution-evidence/v1", **evidence}
        )
        try:
            published = self.client.publish(
                namespace="synthetic-data",
                name=f"execution-evidence-{attempt_id[:24]}",
                kind="synthetic-data.execution-evidence",
                version=f"attempt-{attempt_id[:32]}",
                layers=(ArtifactLayer(media_type=EVIDENCE_MEDIA_TYPE, data=payload),),
                provenance=Provenance(
                    producer_identity="juntai-synthetic-data-worker",
                    source_revision=source_revision,
                    build_id=self.producer_build_id,
                    created_at=_created_at(),
                ),
                idempotency_key=f"synthetic-execution-evidence:{attempt_id}",
                labels={"protocol": "juntai.synthetic.worker/v1"},
                annotations={"job-id": job_id},
                correlation_id=job_id,
            )
        except (ArtifactError, TimeoutError, ConnectionError, ValueError) as error:
            raise SyntheticDataError(
                ErrorCode.PUBLICATION_FAILED,
                "execution evidence Artifact publication failed",
                retryable=True,
            ) from error
        result = published.reference
        return ExactArtifactReference(
            tenantId=tenant_id,
            artifactId=result.artifact_id,
            versionId=result.version_id,
            manifestDigest=result.manifest_digest,
            mediaType=EVIDENCE_MEDIA_TYPE,
            byteLength=len(payload),
            producerBuildId=self.producer_build_id,
        )
