"""Application service coordinating admission, generation, validation, and publication."""

from __future__ import annotations

import re
import uuid
from datetime import UTC, datetime
from decimal import Decimal

from juntai.usage import UsageContext, UsageEvent, UsageReporter, stable_usage_event_id

from juntai_synthetic_data.contracts.models import (
    CreateJobRequest,
    Failure,
    JobResult,
    JobStatus,
    QuotaReservationView,
    validate_idempotency_key,
)
from juntai_synthetic_data.dataset import BoundedDatasetSink
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.execution.coordinator import WorkerCoordinator
from juntai_synthetic_data.jobs import Job, JobRepository, JobState
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.provenance import build_provenance
from juntai_synthetic_data.providers import GenerationExecutionContext, ProviderRegistry
from juntai_synthetic_data.publication import DatasetPublisher
from juntai_synthetic_data.quotas import InMemoryQuotaLedger
from juntai_synthetic_data.validators import ValidatorEvidence, ValidatorSandbox

_TENANT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class SyntheticDataService:
    def __init__(
        self,
        *,
        repository: JobRepository,
        providers: ProviderRegistry,
        policy: DefaultPolicyEngine,
        quotas: InMemoryQuotaLedger,
        publisher: DatasetPublisher,
        validator_sandbox: ValidatorSandbox | None = None,
        usage_reporter: UsageReporter | None = None,
        source_revision: str = "0" * 40,
        coordinator: WorkerCoordinator | None = None,
    ) -> None:
        self.repository = repository
        self.providers = providers
        self.policy = policy
        self.quotas = quotas
        self.publisher = publisher
        self.validator_sandbox = validator_sandbox
        self.usage_reporter = usage_reporter
        self.source_revision = source_revision
        self.coordinator = coordinator

    @staticmethod
    def _tenant(value: str) -> str:
        if not _TENANT.fullmatch(value):
            raise ValueError("tenant identity must be a bounded opaque identifier")
        return value

    def create_job(
        self,
        tenant_id: str,
        idempotency_key: str,
        request: CreateJobRequest,
    ) -> JobStatus:
        tenant_id = self._tenant(tenant_id)
        idempotency_key = validate_idempotency_key(idempotency_key)
        existing = self.repository.find_idempotent(tenant_id, idempotency_key)
        if existing is not None:
            if existing.request_digest != request.digest:
                raise SyntheticDataError(
                    ErrorCode.IDEMPOTENCY_KEY_REUSED,
                    "Idempotency-Key was already used for different content",
                )
            return self.status(existing)
        job = Job(
            job_id=f"job_{uuid.uuid4().hex}",
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            request_digest=request.digest,
            request=request,
        )
        self.repository.create(job)
        expected = job.version
        try:
            job.transition(JobState.POLICY_CHECK)
            decision = self.policy.evaluate(request)
            provider = self.providers.select(request)
            reservation = self.quotas.reserve(tenant_id, job.job_id, request)
            job.quota = {**reservation.to_dict(), "policy_digest": decision.digest}
            job.provider_id = provider.manifest.provider_id
            job.worker_image_digest = provider.manifest.worker_image_digest
            job.transition(JobState.QUEUED)
            if self.coordinator is not None:
                self.coordinator.queue(
                    job,
                    expected_version=expected,
                    provider_version=provider.manifest.version,
                )
                return self.status(job)
        except SyntheticDataError as error:
            job.fail(error)
        self.repository.save(job, expected_version=expected)
        return self.status(job)

    def get_job(self, tenant_id: str, job_id: str) -> Job:
        job = self.repository.get(self._tenant(tenant_id), job_id)
        if job is None:
            raise SyntheticDataError(ErrorCode.JOB_NOT_FOUND, "job not found")
        return job

    def cancel(self, tenant_id: str, job_id: str) -> JobStatus:
        job = self.get_job(tenant_id, job_id)
        expected = job.version
        if not job.terminal and job.state is not JobState.PUBLISHING:
            job.request_cancellation()
            if self.coordinator is not None:
                self.coordinator.cancel(job, expected_version=expected)
                return self.status(job)
            if job.state is JobState.CANCELLING and job.transitions[-2].from_state in {
                JobState.ACCEPTED,
                JobState.POLICY_CHECK,
                JobState.QUEUED,
            }:
                job.transition(JobState.CANCELLED, reason="cancelled before execution")
                self._release(job)
            self.repository.save(job, expected_version=expected)
        return self.status(job)

    def accept_worker_event(self, event, *, authenticated_producer) -> str:
        if self.coordinator is None:
            raise SyntheticDataError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "SWP coordinator is not configured",
                retryable=True,
            )
        job = self.get_job(event.tenant_id, event.job_id)
        disposition = self.coordinator.accept_event(
            job, event, authenticated_producer=authenticated_producer
        )
        if job.terminal:
            self._release(job)
        return disposition

    def run_job(self, tenant_id: str, job_id: str) -> JobStatus:
        job = self.get_job(tenant_id, job_id)
        if job.state is not JobState.QUEUED:
            return self.status(job)
        provider = self.providers.select(job.request)
        expected = job.version
        job.transition(JobState.RUNNING)
        started_at = job.updated_at
        self.repository.save(job, expected_version=expected)

        def cancelled() -> bool:
            current = self.repository.get(job.tenant_id, job.job_id)
            return bool(current and current.cancellation_requested)

        try:
            with BoundedDatasetSink(job.request.generation_contract) as sink:
                dataset = provider.generate(
                    job.request.generation_contract,
                    job.request.seed,
                    sink,
                    GenerationExecutionContext(
                        job_id=job.job_id,
                        tenant_id=job.tenant_id,
                        cancellation_requested=cancelled,
                        deadline_seconds=job.request.provider.requirements.maximum_runtime_seconds,
                    ),
                )
                job = self.get_job(tenant_id, job_id)
                if job.cancellation_requested:
                    raise InterruptedError("generation cancelled")
                validator_evidence: ValidatorEvidence | None = None
                validator_reference: str | None = None
                if job.request.validator is not None:
                    expected = job.version
                    job.transition(JobState.VALIDATING)
                    self.repository.save(job, expected_version=expected)
                    if self.validator_sandbox is None:
                        raise SyntheticDataError(
                            ErrorCode.DEPENDENCY_UNAVAILABLE,
                            "validator sandbox is not configured",
                            retryable=True,
                        )
                    descriptor = job.request.validator
                    validator_reference = (
                        f"{descriptor.artifact_id}/{descriptor.version_id}@{descriptor.digest}"
                    )
                    validator_evidence = self.validator_sandbox.validate(descriptor, dataset)
                    job = self.get_job(tenant_id, job_id)
                expected = job.version
                job.transition(JobState.PUBLISHING)
                self.repository.save(job, expected_version=expected)
                manifest = {
                    "job_id": job.job_id,
                    "request_digest": job.request_digest,
                    "contract_digest": job.request.generation_contract.digest,
                    "logical_dataset_digest": dataset.logical_digest,
                    "format": job.request.generation_contract.output.format,
                    "compression": job.request.generation_contract.output.compression,
                    "record_count": dataset.record_count,
                    "byte_count": dataset.byte_count,
                    "seed": job.request.seed,
                    "provider": {
                        "id": provider.manifest.provider_id,
                        "version": provider.manifest.version,
                    },
                    "validator_reference": validator_reference,
                    "validation_digest": (
                        validator_evidence.digest if validator_evidence else None
                    ),
                    "shards": [
                        {"name": shard.name, "digest": shard.digest, "size": len(shard.data)}
                        for shard in dataset.shards
                    ],
                }
                artifact = self.publisher.publish(
                    job_id=job.job_id,
                    tenant_id=job.tenant_id,
                    idempotency_key=job.idempotency_key,
                    dataset=dataset,
                    manifest=manifest,
                    source_revision=self.source_revision,
                    correlation_id=job.job_id,
                )
                completed_at = datetime.now(UTC)
                quota_id = str(job.quota["reservation_id"]) if job.quota else ""
                policy_digest = str(job.quota["policy_digest"]) if job.quota else ""
                provenance = build_provenance(
                    job_id=job.job_id,
                    request_digest=job.request_digest,
                    contract_digest=job.request.generation_contract.digest,
                    seed=job.request.seed,
                    policy_digest=policy_digest,
                    quota_reservation_id=quota_id,
                    provider=provider.manifest,
                    validator_reference=validator_reference,
                    validator_evidence=validator_evidence,
                    dataset=dataset,
                    artifact=artifact,
                    started_at=started_at,
                    completed_at=completed_at,
                )
                job = self.get_job(tenant_id, job_id)
                expected = job.version
                job.result = {
                    "job_id": job.job_id,
                    "artifact": {
                        "artifact_id": artifact.artifact_id,
                        "version_id": artifact.version_id,
                        "digest": artifact.digest,
                        "media_type": artifact.media_type,
                    },
                    "manifest_digest": dataset.logical_digest,
                    "format": job.request.generation_contract.output.format,
                    "compression": job.request.generation_contract.output.compression,
                    "record_count": dataset.record_count,
                    "byte_count": dataset.byte_count,
                    "seed": job.request.seed,
                    "provenance": provenance,
                    "validator_passed": validator_evidence.passed if validator_evidence else None,
                }
                job.transition(JobState.SUCCEEDED)
                self.repository.save(job, expected_version=expected)
                self._report_usage(job, dataset.record_count, dataset.byte_count, completed_at)
        except InterruptedError:
            job = self.get_job(tenant_id, job_id)
            expected = job.version
            if job.state is not JobState.CANCELLING:
                job.request_cancellation()
            job.transition(JobState.CANCELLED, reason=ErrorCode.JOB_CANCELLED.value)
            self.repository.save(job, expected_version=expected)
        except SyntheticDataError as error:
            job = self.get_job(tenant_id, job_id)
            expected = job.version
            job.fail(error)
            self.repository.save(job, expected_version=expected)
        except Exception as exc:
            job = self.get_job(tenant_id, job_id)
            expected = job.version
            job.fail(
                SyntheticDataError(
                    ErrorCode.DEPENDENCY_UNAVAILABLE,
                    "generation dependency failed",
                    retryable=True,
                    details={"type": type(exc).__name__},
                )
            )
            self.repository.save(job, expected_version=expected)
        job = self.get_job(tenant_id, job_id)
        if job.terminal:
            self._release(job)
        return self.status(job)

    def result(self, tenant_id: str, job_id: str) -> JobResult:
        job = self.get_job(tenant_id, job_id)
        if job.state is not JobState.SUCCEEDED or job.result is None:
            raise SyntheticDataError(ErrorCode.JOB_NOT_SUCCEEDED, "job has no successful result")
        return JobResult.model_validate(job.result)

    def _release(self, job: Job) -> None:
        if job.quota:
            self.quotas.release(str(job.quota["reservation_id"]))

    def _report_usage(
        self,
        job: Job,
        record_count: int,
        byte_count: int,
        occurred_at: datetime,
    ) -> None:
        if self.usage_reporter is None:
            return
        context = UsageContext(
            tenant_id=job.tenant_id,
            service_namespace="juntai",
            service_name="synthetic-data-generation",
            execution_id=job.job_id,
        )
        for meter, quantity, unit, part in (
            ("synthetic_data.records", record_count, "record", "records"),
            ("synthetic_data.bytes", byte_count, "byte", "bytes"),
        ):
            self.usage_reporter.report(
                UsageEvent(
                    event_id=stable_usage_event_id(
                        "juntai-synthetic-data-generation", job.job_id, meter, part
                    ),
                    context=context,
                    meter=meter,
                    quantity=Decimal(quantity),
                    unit=unit,
                    occurred_at=occurred_at,
                    source_id=job.job_id,
                )
            )

    @staticmethod
    def status(job: Job) -> JobStatus:
        quota = None
        if job.quota:
            quota = QuotaReservationView(
                reservation_id=str(job.quota["reservation_id"]),
                records=int(job.quota["records"]),
                bytes=int(job.quota["bytes"]),
                compute_seconds=int(job.quota["compute_seconds"]),
                provider_class=str(job.quota["provider_class"]),
            )
        failure = Failure.model_validate(job.failure) if job.failure else None
        return JobStatus(
            job_id=job.job_id,
            state=job.state.value,
            stage=job.state.value.lower(),
            request_digest=job.request_digest,
            version=job.version,
            created_at=job.created_at,
            updated_at=job.updated_at,
            quota=quota,
            failure=failure,
        )
