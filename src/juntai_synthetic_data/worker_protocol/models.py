"""Service-owned Synthetic Worker Protocol v1 envelopes and canonicalization."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Annotated, Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

PROTOCOL_VERSION = "juntai.synthetic.worker/v1"
INPUT_MEDIA_TYPE = "application/vnd.juntai.synthetic.worker-input.v1+json"
EVIDENCE_MEDIA_TYPE = "application/vnd.juntai.synthetic.execution-evidence.v1+json"
SOCKET_PATH = "/var/run/juntai-worker/swp-v1.sock"
MAX_FRAME_BYTES = 1_048_576
VISIBILITY_SECONDS = 60
RENEW_EVERY_SECONDS = 20
MAXIMUM_DELIVERIES = 5
RETRY_BASE_SECONDS = 5
RETRY_CAP_SECONDS = 300
TERMINATION_ALLOWANCE_SECONDS = 60
MAX_LEASE_SECONDS = 6 * 60 * 60
REQUIRED_CAPABILITIES = (
    "canonical-envelope-digest",
    "cancel-sequence",
    "exact-artifact-references",
    "terminal-evidence",
)

Digest = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]
OpaqueId = Annotated[str, StringConstraints(min_length=1, max_length=200)]


class ProtocolError(ValueError):
    """A stable fail-closed SWP contract violation."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _reject_duplicate_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ProtocolError("ENVELOPE_INVALID", f"duplicate JSON member: {key}")
        result[key] = value
    return result


def parse_json(payload: bytes) -> dict[str, Any]:
    """Parse one UTF-8 JSON object while rejecting duplicate members and non-finite numbers."""

    if payload.startswith(b"\xef\xbb\xbf"):
        raise ProtocolError("ENVELOPE_INVALID", "UTF-8 BOM is forbidden")
    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=_reject_duplicate_pairs,
            parse_constant=lambda value: (_ for _ in ()).throw(
                ProtocolError("ENVELOPE_INVALID", f"non-finite number: {value}")
            ),
        )
    except ProtocolError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ProtocolError("ENVELOPE_INVALID", "invalid UTF-8 JSON envelope") from error
    if not isinstance(value, dict):
        raise ProtocolError("ENVELOPE_INVALID", "envelope must be a JSON object")
    return value


def canonical_json(value: object) -> bytes:
    """Return the RFC 8785 representation for the integer/string-only SWP schema.

    SWP deliberately excludes floating-point envelope members. This avoids cross-language
    number formatting ambiguity while retaining full RFC 8785 canonical bytes for the
    permitted JSON value set.
    """

    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", by_alias=True, exclude_none=True)

    def string(item: str) -> str:
        try:
            item.encode("utf-8", errors="strict")
        except UnicodeEncodeError as error:
            raise ProtocolError(
                "ENVELOPE_INVALID", "unpaired Unicode surrogate is forbidden"
            ) from error
        return json.dumps(item, ensure_ascii=False, allow_nan=False, separators=(",", ":"))

    def encode(item: object) -> str:
        if isinstance(item, float):
            raise ProtocolError("ENVELOPE_INVALID", "floating-point envelope values are forbidden")
        if item is None:
            return "null"
        if item is True:
            return "true"
        if item is False:
            return "false"
        if isinstance(item, int):
            if not -(2**53 - 1) <= item <= 2**53 - 1:
                raise ProtocolError(
                    "ENVELOPE_INVALID", "integer exceeds the interoperable JSON range"
                )
            return str(item)
        if isinstance(item, str):
            return string(item)
        if isinstance(item, dict):
            if any(not isinstance(key, str) for key in item):
                raise ProtocolError("ENVELOPE_INVALID", "JSON object keys must be strings")
            ordered = sorted(item, key=lambda key: key.encode("utf-16-be"))
            return "{" + ",".join(f"{string(key)}:{encode(item[key])}" for key in ordered) + "}"
        if isinstance(item, (list, tuple)):
            return "[" + ",".join(encode(child) for child in item) + "]"
        raise ProtocolError("ENVELOPE_INVALID", "envelope is not canonicalizable")

    try:
        return encode(value).encode("utf-8")
    except ProtocolError:
        raise
    except (TypeError, ValueError, UnicodeEncodeError) as error:
        raise ProtocolError("ENVELOPE_INVALID", "envelope is not canonicalizable") from error


class StrictProtocolModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


class WorkloadIdentity(StrictProtocolModel):
    namespace: OpaqueId
    service_account: OpaqueId = Field(alias="serviceAccount")


class ExactArtifactReference(StrictProtocolModel):
    tenant_id: OpaqueId = Field(alias="tenantId")
    artifact_id: OpaqueId = Field(alias="artifactId")
    version_id: OpaqueId = Field(alias="versionId")
    manifest_digest: Digest = Field(alias="manifestDigest")
    media_type: Annotated[str, StringConstraints(min_length=1, max_length=200)] = Field(
        alias="mediaType"
    )
    byte_length: int = Field(alias="byteLength", ge=0, le=1_073_741_824)
    producer_build_id: OpaqueId = Field(alias="producerBuildId")


class ResourceEnvelope(StrictProtocolModel):
    cpu_millis: int = Field(alias="cpuMillis", gt=0, le=64_000)
    memory_bytes: int = Field(alias="memoryBytes", gt=0, le=68_719_476_736)
    ephemeral_bytes: int = Field(alias="ephemeralBytes", gt=0, le=1_073_741_824_000)
    process_limit: int = Field(alias="processLimit", gt=0, le=4096)


class RetryPolicy(StrictProtocolModel):
    maximum_deliveries: Literal[5] = Field(default=5, alias="maximumDeliveries")
    base_seconds: Literal[5] = Field(default=5, alias="baseSeconds")
    cap_seconds: Literal[300] = Field(default=300, alias="capSeconds")


class ProgressBounds(StrictProtocolModel):
    completed: int = Field(ge=0)
    total: int = Field(ge=0)

    @model_validator(mode="after")
    def ordered(self) -> Self:
        if self.completed > self.total:
            raise ValueError("progress completed cannot exceed total")
        return self


class StableWorkerError(StrictProtocolModel):
    code: Literal[
        "PROTOCOL_UNSUPPORTED",
        "ENVELOPE_INVALID",
        "ENVELOPE_DIGEST_MISMATCH",
        "IDENTITY_MISMATCH",
        "TENANT_MISMATCH",
        "ATTEMPT_STALE",
        "LEASE_EXPIRED",
        "WORKER_EXITED",
        "DELIVERY_EXHAUSTED",
        "RESULT_DUPLICATE",
        "RESULT_CONFLICT",
        "DEADLINE_EXCEEDED",
        "CANCELLED",
        "ARTIFACT_INTEGRITY_FAILED",
        "PUBLICATION_FAILED",
        "DEPENDENCY_UNAVAILABLE",
    ]
    retryable: bool
    message: Annotated[str, StringConstraints(min_length=1, max_length=500)]


class EnvelopeBase(StrictProtocolModel):
    protocol_version: Literal[PROTOCOL_VERSION] = Field(
        default=PROTOCOL_VERSION, alias="protocolVersion"
    )
    message_id: OpaqueId = Field(alias="messageId")
    tenant_id: OpaqueId = Field(alias="tenantId")
    job_id: OpaqueId = Field(alias="jobId")
    attempt_id: OpaqueId = Field(alias="attemptId")
    attempt_number: int = Field(alias="attemptNumber", ge=1)
    sequence: int = Field(ge=0)
    emitted_at: datetime = Field(alias="emittedAt")
    deadline: datetime
    correlation_id: OpaqueId = Field(alias="correlationId")
    traceparent: Annotated[str, StringConstraints(max_length=200)] | None = None
    producer_workload: WorkloadIdentity = Field(alias="producerWorkload")
    content_digest: Digest | None = Field(default=None, alias="contentDigest")

    def content(self) -> dict[str, Any]:
        return self.model_dump(
            mode="json", by_alias=True, exclude_none=True, exclude={"content_digest"}
        )

    def computed_digest(self) -> str:
        return f"sha256:{hashlib.sha256(canonical_json(self.content())).hexdigest()}"

    def signed(self) -> Self:
        return self.model_copy(update={"content_digest": self.computed_digest()})

    def verify(self, *, authenticated_producer: WorkloadIdentity | None = None) -> None:
        if self.content_digest is None or self.content_digest != self.computed_digest():
            raise ProtocolError("ENVELOPE_DIGEST_MISMATCH", "contentDigest does not match content")
        if self.deadline < self.emitted_at:
            raise ProtocolError("ENVELOPE_INVALID", "deadline precedes emittedAt")
        if authenticated_producer is not None and authenticated_producer != self.producer_workload:
            raise ProtocolError(
                "IDENTITY_MISMATCH", "transport principal differs from producerWorkload"
            )

    def canonical_bytes(self) -> bytes:
        self.verify()
        payload = canonical_json(self)
        if len(payload) > MAX_FRAME_BYTES:
            raise ProtocolError("ENVELOPE_INVALID", "envelope exceeds maximum frame length")
        return payload


class DispatchEnvelope(EnvelopeBase):
    kind: Literal["dispatch"] = "dispatch"
    request_digest: Digest = Field(alias="requestDigest")
    input_artifact: ExactArtifactReference = Field(alias="inputArtifact")
    provider_id: OpaqueId = Field(alias="providerId")
    provider_version: OpaqueId = Field(alias="providerVersion")
    worker_image_digest: Digest = Field(alias="workerImageDigest")
    required_capabilities: tuple[OpaqueId, ...] = Field(alias="requiredCapabilities", max_length=32)
    min_executor_binding: OpaqueId = Field(alias="minExecutorBinding")
    resource_envelope: ResourceEnvelope = Field(alias="resourceEnvelope")
    retry_policy: RetryPolicy = Field(alias="retryPolicy", default_factory=RetryPolicy)
    idempotency_key_digest: Digest = Field(alias="idempotencyKeyDigest")


class CancelEnvelope(EnvelopeBase):
    kind: Literal["cancel"] = "cancel"
    cancel_sequence: int = Field(alias="cancelSequence", ge=1)
    requested_at: datetime = Field(alias="requestedAt")
    reason_code: OpaqueId = Field(alias="reasonCode")
    requested_by_kind: Literal["human", "delegated", "workload", "policy"] = Field(
        alias="requestedByKind"
    )
    grace_deadline: datetime = Field(alias="graceDeadline")


class WorkerEventEnvelope(EnvelopeBase):
    kind: Literal["worker-event"] = "worker-event"
    event_id: OpaqueId = Field(alias="eventId")
    event_type: Literal["STARTED", "STAGE", "TERMINAL"] = Field(alias="eventType")
    execution_lease_id: OpaqueId = Field(alias="executionLeaseId")
    stage: Literal["RUNNING", "VALIDATING", "PUBLISHING"]
    progress_bounds: ProgressBounds = Field(alias="progressBounds")
    observed_cancel_sequence: int = Field(alias="observedCancelSequence", ge=0)
    worker_image_digest: Digest = Field(alias="workerImageDigest")
    protocol_capabilities: tuple[OpaqueId, ...] = Field(alias="protocolCapabilities", max_length=32)
    evidence_counters: dict[str, int] = Field(alias="evidenceCounters", max_length=32)
    outcome: Literal["SUCCEEDED", "FAILED", "CANCELLED", "DEADLINE_EXCEEDED"] | None = None
    error: StableWorkerError | None = None
    dataset_artifact: ExactArtifactReference | None = Field(default=None, alias="datasetArtifact")
    execution_evidence_artifact: ExactArtifactReference | None = Field(
        default=None, alias="executionEvidenceArtifact"
    )
    started_at: datetime | None = Field(default=None, alias="startedAt")
    terminal_at: datetime | None = Field(default=None, alias="terminalAt")
    output_records: int | None = Field(default=None, alias="outputRecords", ge=0)
    output_bytes: int | None = Field(default=None, alias="outputBytes", ge=0)
    consumed_input_digest: Digest | None = Field(default=None, alias="consumedInputDigest")

    @model_validator(mode="after")
    def validate_event_shape(self) -> Self:
        terminal = self.event_type == "TERMINAL"
        if terminal != (self.outcome is not None):
            raise ValueError("TERMINAL events require exactly one outcome")
        if terminal and (
            self.execution_evidence_artifact is None
            or self.started_at is None
            or self.terminal_at is None
            or self.consumed_input_digest is None
        ):
            raise ValueError("TERMINAL events require evidence, times, and consumed input digest")
        if self.outcome == "SUCCEEDED" and self.dataset_artifact is None:
            raise ValueError("SUCCEEDED requires a dataset Artifact")
        if self.outcome not in {None, "SUCCEEDED"} and self.error is None:
            raise ValueError("non-success terminal events require a stable error")
        if (
            self.terminal_at is not None
            and self.started_at is not None
            and self.terminal_at < self.started_at
        ):
            raise ValueError("terminalAt precedes startedAt")
        if any(value < 0 for value in self.evidence_counters.values()):
            raise ValueError("evidence counters must be non-negative")
        return self


Envelope = DispatchEnvelope | CancelEnvelope | WorkerEventEnvelope


def decode_envelope(payload: bytes) -> Envelope:
    value = parse_json(payload)
    if len(payload) > MAX_FRAME_BYTES:
        raise ProtocolError("ENVELOPE_INVALID", "envelope exceeds maximum frame length")
    kind = value.get("kind")
    model: type[EnvelopeBase]
    if kind == "dispatch":
        model = DispatchEnvelope
    elif kind == "cancel":
        model = CancelEnvelope
    elif kind == "worker-event":
        model = WorkerEventEnvelope
    else:
        raise ProtocolError("PROTOCOL_UNSUPPORTED", "unknown envelope kind or protocol major")
    try:
        envelope = model.model_validate(value)
    except ValueError as error:
        raise ProtocolError(
            "ENVELOPE_INVALID", "envelope does not satisfy SWP/v1 schema"
        ) from error
    envelope.verify()
    if envelope.canonical_bytes() != payload:
        raise ProtocolError("ENVELOPE_INVALID", "envelope bytes are not canonical RFC 8785 JSON")
    return envelope  # type: ignore[return-value]


def retry_delay_upper_bound(delivery_count: int) -> int:
    if delivery_count < 1 or delivery_count > MAXIMUM_DELIVERIES:
        raise ValueError("delivery count is outside the SWP/v1 budget")
    return min(RETRY_CAP_SECONDS, RETRY_BASE_SECONDS * 2 ** (delivery_count - 1))
