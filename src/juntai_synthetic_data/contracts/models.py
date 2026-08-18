"""Public generic contracts with deliberately bounded structural vocabulary."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, model_validator

REQUEST_VERSION = "juntai.synthetic-data.request/v1"
CONTRACT_VERSION = "juntai.synthetic-data.contract/v1"
MANIFEST_VERSION = "juntai.synthetic-data.dataset-manifest/v1"
PROVENANCE_VERSION = "juntai.synthetic-data.provenance/v1"

Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,62}$")]
Seed = Annotated[str, StringConstraints(min_length=1, max_length=256)]
Digest = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]


def canonical_json(value: object) -> bytes:
    """Return the platform canonical JSON representation."""

    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude_none=True)
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")


def canonical_digest(value: object) -> str:
    return f"sha256:{hashlib.sha256(canonical_json(value)).hexdigest()}"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class FieldType(StrEnum):
    STRING = "string"
    INTEGER = "integer"
    NUMBER = "number"
    BOOLEAN = "boolean"
    DATE = "date"
    DATETIME = "datetime"


class DistributionKind(StrEnum):
    CONSTANT = "constant"
    SEQUENCE = "sequence"
    CHOICE = "choice"
    UNIFORM = "uniform"
    NORMAL = "normal"
    UUID = "uuid"


class Distribution(StrictModel):
    kind: DistributionKind
    value: Any | None = None
    values: tuple[Any, ...] = Field(default=(), max_length=100)
    minimum: float | None = None
    maximum: float | None = None
    mean: float | None = None
    stddev: float | None = Field(default=None, gt=0)
    start: int | None = None
    step: int | None = None

    @model_validator(mode="after")
    def validate_shape(self) -> Distribution:
        if self.kind is DistributionKind.CONSTANT and self.value is None:
            raise ValueError("constant distribution requires value")
        if self.kind is DistributionKind.CHOICE and not self.values:
            raise ValueError("choice distribution requires values")
        if self.kind is DistributionKind.UNIFORM and (
            self.minimum is None or self.maximum is None or self.minimum > self.maximum
        ):
            raise ValueError("uniform distribution requires ordered minimum and maximum")
        if self.kind is DistributionKind.NORMAL and (self.mean is None or self.stddev is None):
            raise ValueError("normal distribution requires mean and stddev")
        return self


class FieldSpec(StrictModel):
    type: FieldType
    nullable: bool = False
    unique: bool = False
    distribution: Distribution | None = None
    metadata: dict[str, str] = Field(default_factory=dict, max_length=32)

    @model_validator(mode="after")
    def bound_metadata(self) -> FieldSpec:
        if any(
            len(key.encode()) > 80 or len(value.encode()) > 500
            for key, value in self.metadata.items()
        ):
            raise ValueError("field metadata keys or values exceed bounds")
        return self


class CountSpec(StrictModel):
    maximum: int = Field(gt=0, le=1_000_000)


class RecordSpec(StrictModel):
    record_type: Identifier
    count: CountSpec
    fields: dict[Identifier, FieldSpec] = Field(min_length=1, max_length=256)


class RelationSpec(StrictModel):
    from_field: str = Field(alias="from", pattern=r"^[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$")
    to_field: str = Field(alias="to", pattern=r"^[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$")
    required: bool = True


class DatasetBounds(StrictModel):
    max_records: int = Field(gt=0, le=1_000_000)
    max_bytes: int = Field(gt=0, le=1_073_741_824)
    max_shards: int = Field(default=8, gt=0, le=15)


class OutputSpec(StrictModel):
    format: Literal["jsonl", "csv"] = "jsonl"
    compression: Literal["none", "gzip"] = "none"


class GenerationContract(StrictModel):
    contract_version: Literal[CONTRACT_VERSION] = CONTRACT_VERSION
    records: tuple[RecordSpec, ...] = Field(min_length=1, max_length=64)
    relations: tuple[RelationSpec, ...] = Field(default=(), max_length=256)
    bounds: DatasetBounds
    output: OutputSpec = Field(default_factory=OutputSpec)
    metadata: dict[str, str] = Field(default_factory=dict, max_length=64)

    @model_validator(mode="after")
    def validate_graph_and_bounds(self) -> GenerationContract:
        record_map = {record.record_type: record for record in self.records}
        if len(record_map) != len(self.records):
            raise ValueError("record_type values must be unique")
        if sum(record.count.maximum for record in self.records) > self.bounds.max_records:
            raise ValueError("declared record counts exceed max_records")
        for relation in self.relations:
            source_type, source_field = relation.from_field.split(".", 1)
            target_type, target_field = relation.to_field.split(".", 1)
            if source_type not in record_map or target_type not in record_map:
                raise ValueError("relation references an unknown record type")
            if source_field not in record_map[source_type].fields:
                raise ValueError("relation source field is unknown")
            if target_field not in record_map[target_type].fields:
                raise ValueError("relation target field is unknown")
            if not record_map[target_type].fields[target_field].unique:
                raise ValueError("relation target field must be unique")
        return self

    @property
    def digest(self) -> str:
        return canonical_digest(self)


class ProviderRequirements(StrictModel):
    deterministic: bool = True
    modes: tuple[str, ...] = ("tabular",)
    maximum_runtime_seconds: int = Field(default=300, gt=0, le=3600)


class ProviderRequest(StrictModel):
    provider_class: str = Field(alias="class", pattern=r"^[a-z][a-z0-9.-]{0,62}$")
    requirements: ProviderRequirements = Field(default_factory=ProviderRequirements)


class PolicyRequest(StrictModel):
    data_classification: Literal["synthetic", "internal", "confidential", "restricted"]
    source_examples: Literal["none", "minimized"] = "none"
    authorization_reference: str | None = Field(default=None, max_length=256)


class ValidatorDescriptor(StrictModel):
    artifact_id: str = Field(min_length=1, max_length=128)
    version_id: str = Field(min_length=1, max_length=128)
    digest: Digest
    media_type: str = Field(default="application/vnd.juntai.validator.v1+tar", max_length=200)
    runtime: Literal["python", "wasm"] = "python"
    entry_point: str = Field(pattern=r"^[A-Za-z_][A-Za-z0-9_.]*:[A-Za-z_][A-Za-z0-9_]*$")
    contract_version: Literal[CONTRACT_VERSION] = CONTRACT_VERSION
    input_protocol: Literal["juntai.synthetic-data.validator-input/v1"] = (
        "juntai.synthetic-data.validator-input/v1"
    )
    output_protocol: Literal["juntai.synthetic-data.validator-output/v1"] = (
        "juntai.synthetic-data.validator-output/v1"
    )
    cpu_millis: int = Field(default=500, gt=0, le=4000)
    memory_bytes: int = Field(default=268_435_456, gt=0, le=2_147_483_648)
    timeout_seconds: int = Field(default=30, gt=0, le=300)
    deterministic: bool = True


class CreateJobRequest(StrictModel):
    contract_version: Literal[REQUEST_VERSION] = REQUEST_VERSION
    generation_contract: GenerationContract
    seed: Seed
    provider: ProviderRequest
    policy: PolicyRequest
    validator: ValidatorDescriptor | None = None

    @property
    def digest(self) -> str:
        return canonical_digest(self)


class Failure(StrictModel):
    code: str
    message: str
    retryable: bool = False
    details: dict[str, str] = Field(default_factory=dict)


class QuotaReservationView(StrictModel):
    reservation_id: str
    records: int
    bytes: int
    compute_seconds: int
    provider_class: str


class ArtifactReferenceView(StrictModel):
    artifact_id: str
    version_id: str
    digest: Digest
    media_type: str


class ProvenanceView(StrictModel):
    schema_version: Literal[PROVENANCE_VERSION] = PROVENANCE_VERSION
    job_id: str
    request_digest: Digest
    contract_digest: Digest
    provider_id: str
    provider_version: str
    model_identity: str | None = None
    model_version: str | None = None
    seed: str
    policy_digest: Digest
    quota_reservation_id: str
    worker_image_digest: Digest
    validator_reference: str | None = None
    validation_digest: Digest | None = None
    logical_dataset_digest: Digest
    artifact_digest: Digest
    record_count: int
    byte_count: int
    shard_count: int
    started_at: datetime
    completed_at: datetime


class JobStatus(StrictModel):
    job_id: str
    state: str
    stage: str
    request_digest: Digest
    version: int
    created_at: datetime
    updated_at: datetime
    quota: QuotaReservationView | None = None
    failure: Failure | None = None


class JobResult(StrictModel):
    job_id: str
    artifact: ArtifactReferenceView
    manifest_digest: Digest
    format: str
    compression: str
    record_count: int
    byte_count: int
    seed: str
    provenance: ProvenanceView
    validator_passed: bool | None = None


def validate_idempotency_key(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9._~:-]{0,198}[A-Za-z0-9])?", value):
        raise ValueError("Idempotency-Key must be a bounded opaque identifier")
    return value
