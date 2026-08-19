"""Repository-free generation engine used only inside the isolated worker process."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from juntai_synthetic_data.dataset import BoundedDatasetSink
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.providers import GenerationExecutionContext, ProviderRegistry
from juntai_synthetic_data.publication import DatasetPublisher
from juntai_synthetic_data.worker_protocol import DispatchEnvelope, ExactArtifactReference

from .artifacts import ExecutionEvidencePublisher, ExecutionInputResolver


@dataclass(frozen=True)
class WorkerExecutionResult:
    dataset_artifact: ExactArtifactReference
    evidence_artifact: ExactArtifactReference
    record_count: int
    byte_count: int
    started_at: datetime
    terminal_at: datetime


class WorkerEngine(Protocol):
    def execute(
        self,
        dispatch: DispatchEnvelope,
        *,
        cancellation_requested: Callable[[], bool],
        stage: Callable[[str, int, int], None],
    ) -> WorkerExecutionResult: ...

    def failure_evidence(
        self,
        dispatch: DispatchEnvelope,
        *,
        code: str,
        started_at: datetime,
        terminal_at: datetime,
    ) -> ExactArtifactReference: ...


class SyntheticWorkerEngine:
    def __init__(
        self,
        *,
        inputs: ExecutionInputResolver,
        providers: ProviderRegistry,
        publisher: DatasetPublisher,
        evidence: ExecutionEvidencePublisher,
        source_revision: str,
    ) -> None:
        self.inputs = inputs
        self.providers = providers
        self.publisher = publisher
        self.evidence = evidence
        self.source_revision = source_revision

    def execute(
        self,
        dispatch: DispatchEnvelope,
        *,
        cancellation_requested: Callable[[], bool],
        stage: Callable[[str, int, int], None],
    ) -> WorkerExecutionResult:
        started_at = datetime.now(UTC)
        request = self.inputs.resolve_input(dispatch.input_artifact)
        if request.digest != dispatch.request_digest:
            raise SyntheticDataError(
                ErrorCode.CONTRACT_INVALID,
                "dispatch and input Artifact request digests differ",
                details={"protocol_error": "ARTIFACT_INTEGRITY_FAILED"},
            )
        provider = self.providers.select(request)
        if (
            provider.manifest.provider_id != dispatch.provider_id
            or provider.manifest.version != dispatch.provider_version
            or provider.manifest.worker_image_digest != dispatch.worker_image_digest
        ):
            raise SyntheticDataError(
                ErrorCode.PROVIDER_UNSUPPORTED,
                "dispatch provider or worker image pin is unavailable",
            )
        if request.validator is not None:
            raise SyntheticDataError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "validator execution requires the released isolated validator binding",
                retryable=False,
            )
        stage("RUNNING", 0, request.generation_contract.bounds.max_records)
        with BoundedDatasetSink(request.generation_contract) as sink:
            dataset = provider.generate(
                request.generation_contract,
                request.seed,
                sink,
                GenerationExecutionContext(
                    job_id=dispatch.job_id,
                    tenant_id=dispatch.tenant_id,
                    cancellation_requested=cancellation_requested,
                    deadline_seconds=request.provider.requirements.maximum_runtime_seconds,
                ),
            )
            if cancellation_requested():
                raise InterruptedError("worker cancellation observed")
            stage("PUBLISHING", dataset.record_count, dataset.record_count)
            manifest = {
                "job_id": dispatch.job_id,
                "request_digest": request.digest,
                "contract_digest": request.generation_contract.digest,
                "logical_dataset_digest": dataset.logical_digest,
                "format": request.generation_contract.output.format,
                "compression": request.generation_contract.output.compression,
                "record_count": dataset.record_count,
                "byte_count": dataset.byte_count,
                "seed": request.seed,
                "provider": {
                    "id": provider.manifest.provider_id,
                    "version": provider.manifest.version,
                },
                "validator_reference": None,
                "validation_digest": None,
                "shards": [
                    {"name": item.name, "digest": item.digest, "size": len(item.data)}
                    for item in dataset.shards
                ],
            }
            published = self.publisher.publish(
                job_id=dispatch.job_id,
                tenant_id=dispatch.tenant_id,
                idempotency_key=dispatch.attempt_id,
                dataset=dataset,
                manifest=manifest,
                source_revision=self.source_revision,
                correlation_id=dispatch.correlation_id,
            )
            terminal_at = datetime.now(UTC)
            evidence = self.evidence.publish_evidence(
                tenant_id=dispatch.tenant_id,
                job_id=dispatch.job_id,
                attempt_id=dispatch.attempt_id,
                source_revision=self.source_revision,
                evidence={
                    "attemptId": dispatch.attempt_id,
                    "requestDigest": request.digest,
                    "contractDigest": request.generation_contract.digest,
                    "workerImageDigest": dispatch.worker_image_digest,
                    "providerId": dispatch.provider_id,
                    "providerVersion": dispatch.provider_version,
                    "seed": request.seed,
                    "logicalDatasetDigest": dataset.logical_digest,
                    "datasetArtifactDigest": published.digest,
                    "recordCount": dataset.record_count,
                    "byteCount": dataset.byte_count,
                    "startedAt": started_at.isoformat(timespec="milliseconds").replace(
                        "+00:00", "Z"
                    ),
                    "terminalAt": terminal_at.isoformat(timespec="milliseconds").replace(
                        "+00:00", "Z"
                    ),
                },
            )
            dataset_reference = ExactArtifactReference(
                tenantId=dispatch.tenant_id,
                artifactId=published.artifact_id,
                versionId=published.version_id,
                manifestDigest=published.digest,
                mediaType=published.media_type,
                byteLength=dataset.byte_count,
                producerBuildId=self.source_revision,
            )
            return WorkerExecutionResult(
                dataset_artifact=dataset_reference,
                evidence_artifact=evidence,
                record_count=dataset.record_count,
                byte_count=dataset.byte_count,
                started_at=started_at,
                terminal_at=terminal_at,
            )

    def failure_evidence(
        self,
        dispatch: DispatchEnvelope,
        *,
        code: str,
        started_at: datetime,
        terminal_at: datetime,
    ) -> ExactArtifactReference:
        return self.evidence.publish_evidence(
            tenant_id=dispatch.tenant_id,
            job_id=dispatch.job_id,
            attempt_id=dispatch.attempt_id,
            source_revision=self.source_revision,
            evidence={
                "attemptId": dispatch.attempt_id,
                "requestDigest": dispatch.request_digest,
                "workerImageDigest": dispatch.worker_image_digest,
                "providerId": dispatch.provider_id,
                "providerVersion": dispatch.provider_version,
                "stableError": code,
                "startedAt": started_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                "terminalAt": terminal_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            },
        )
