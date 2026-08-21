"""Versioned synchronous generation contracts with a bounded structural vocabulary."""

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

Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]{0,62}$")]
DestinationIdentifier = Annotated[str, StringConstraints(min_length=1, max_length=63)]
Seed = Annotated[str, StringConstraints(min_length=1, max_length=256)]
Digest = Annotated[str, StringConstraints(pattern=r"^sha256:[0-9a-f]{64}$")]


def canonical_json(value: object) -> bytes:
    if isinstance(value, BaseModel):
        value = value.model_dump(mode="json", exclude_none=True, by_alias=True)
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
    model_config = ConfigDict(extra="forbid", frozen=True, populate_by_name=True)


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
        if self.kind is DistributionKind.SEQUENCE and self.step == 0:
            raise ValueError("sequence distribution step cannot be zero")
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


class DestinationSpec(StrictModel):
    schema_name: DestinationIdentifier = Field(alias="schema")
    table: DestinationIdentifier
    columns: dict[Identifier, DestinationIdentifier] = Field(min_length=1, max_length=256)
    key_fields: tuple[Identifier, ...] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def validate_mapping(self) -> DestinationSpec:
        if len(set(self.columns.values())) != len(self.columns):
            raise ValueError("destination columns must be mapped at most once")
        if len(set(self.key_fields)) != len(self.key_fields):
            raise ValueError("destination key_fields must be unique")
        missing = set(self.key_fields) - set(self.columns)
        if missing:
            raise ValueError("destination key_fields must be present in columns")
        return self


class RecordSpec(StrictModel):
    record_type: Identifier
    count: int = Field(gt=0, le=1_000_000)
    destination: DestinationSpec
    fields: dict[Identifier, FieldSpec] = Field(min_length=1, max_length=256)

    @model_validator(mode="after")
    def validate_destination_fields(self) -> RecordSpec:
        unknown = set(self.destination.columns) - set(self.fields)
        if unknown:
            raise ValueError("destination columns reference unknown generated fields")
        unmapped = set(self.fields) - set(self.destination.columns)
        if unmapped:
            raise ValueError("every generated field must map to a destination column")
        for key_field in self.destination.key_fields:
            if self.fields[key_field].nullable:
                raise ValueError("destination key fields cannot be nullable")
        return self


class RelationSpec(StrictModel):
    from_field: str = Field(alias="from", pattern=r"^[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$")
    to_field: str = Field(alias="to", pattern=r"^[a-z][a-z0-9_]{0,62}\.[a-z][a-z0-9_]{0,62}$")
    required: bool = True


class DatasetBounds(StrictModel):
    max_records: int = Field(gt=0, le=1_000_000)
    max_bytes: int = Field(gt=0, le=1_073_741_824)


class GenerationContract(StrictModel):
    contract_version: Literal[CONTRACT_VERSION] = CONTRACT_VERSION
    records: tuple[RecordSpec, ...] = Field(min_length=1, max_length=64)
    relations: tuple[RelationSpec, ...] = Field(default=(), max_length=256)
    bounds: DatasetBounds
    metadata: dict[str, str] = Field(default_factory=dict, max_length=64)

    @model_validator(mode="after")
    def validate_graph_and_bounds(self) -> GenerationContract:
        record_map = {record.record_type: record for record in self.records}
        if len(record_map) != len(self.records):
            raise ValueError("record_type values must be unique")
        if sum(record.count for record in self.records) > self.bounds.max_records:
            raise ValueError("declared record counts exceed max_records")
        edges: dict[str, set[str]] = {name: set() for name in record_map}
        for relation in self.relations:
            source_type, source_field = relation.from_field.split(".", 1)
            target_type, target_field = relation.to_field.split(".", 1)
            if source_type not in record_map or target_type not in record_map:
                raise ValueError("relation references an unknown record type")
            source = record_map[source_type].fields.get(source_field)
            target = record_map[target_type].fields.get(target_field)
            if source is None or target is None:
                raise ValueError("relation references an unknown field")
            if source.type is not target.type:
                raise ValueError("relation fields must have the same structural type")
            if not target.unique:
                raise ValueError("relation target field must be unique")
            if relation.required and source.nullable:
                raise ValueError("required relation source field cannot be nullable")
            if source_type != target_type:
                edges[target_type].add(source_type)
        pending = {name: set() for name in edges}
        for parent, children in edges.items():
            for child in children:
                pending[child].add(parent)
        ready = [record.record_type for record in self.records if not pending[record.record_type]]
        visited: list[str] = []
        while ready:
            current = ready.pop(0)
            visited.append(current)
            for child in sorted(edges[current]):
                pending[child].discard(current)
                if not pending[child] and child not in ready and child not in visited:
                    ready.append(child)
        if len(visited) != len(record_map):
            raise ValueError("relation graph must be acyclic in V1")
        return self

    @property
    def digest(self) -> str:
        return canonical_digest(self)


class ProviderRequirements(StrictModel):
    deterministic: Literal[True] = True
    modes: tuple[str, ...] = ("tabular",)
    maximum_runtime_seconds: int = Field(default=300, gt=0, le=3600)


class ProviderRequest(StrictModel):
    provider_class: str = Field(alias="class", pattern=r"^[a-z][a-z0-9.-]{0,62}$")
    requirements: ProviderRequirements = Field(default_factory=ProviderRequirements)


class PolicyRequest(StrictModel):
    data_classification: Literal["synthetic", "internal"] = "synthetic"


class CreateGenerationRequest(StrictModel):
    contract_version: Literal[REQUEST_VERSION] = REQUEST_VERSION
    generation_contract: GenerationContract
    seed: Seed
    provider: ProviderRequest
    policy: PolicyRequest = Field(default_factory=PolicyRequest)

    @property
    def digest(self) -> str:
        return canonical_digest(self)


class GenerationState(StrEnum):
    COMMITTED = "COMMITTED"
    DELETED = "DELETED"


class ProviderView(StrictModel):
    provider_class: str = Field(alias="class")
    provider_id: str
    version: str


class DestinationResult(StrictModel):
    schema_name: DestinationIdentifier = Field(alias="schema")
    table: DestinationIdentifier
    records_written: int = Field(ge=0)


class GenerationResult(StrictModel):
    generation_id: str = Field(pattern=r"^gen_[0-9a-f]{32}$")
    state: GenerationState
    request_digest: Digest
    contract_digest: Digest
    data_digest: Digest
    seed: Seed
    provider: ProviderView
    destinations: tuple[DestinationResult, ...]
    record_count: int = Field(ge=0)
    byte_count: int = Field(ge=0)
    created_at: datetime
    deleted_at: datetime | None = None


def validate_idempotency_key(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9](?:[A-Za-z0-9._~:-]{0,198}[A-Za-z0-9])?", value):
        raise ValueError("Idempotency-Key must be a bounded opaque identifier")
    return value
