"""Reference schema-only deterministic tabular provider."""

from __future__ import annotations

import hashlib
import random
import uuid
from datetime import UTC, date, datetime, timedelta
from typing import Any

from juntai_synthetic_data.contracts.models import (
    CONTRACT_VERSION,
    DistributionKind,
    FieldSpec,
    FieldType,
    GenerationContract,
)
from juntai_synthetic_data.dataset import GeneratedDataset, validate_generated_dataset
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

from .base import GeneratorProviderManifest


class DeterministicTabularProvider:
    def __init__(self) -> None:
        self.manifest = GeneratorProviderManifest(
            provider_id="juntai.synthetic-data.tabular",
            version="1.0.0",
            provider_class="tabular",
            contract_versions=frozenset({CONTRACT_VERSION}),
            generation_modes=frozenset({"tabular"}),
            deterministic_seed=True,
            privacy_classes=frozenset({"synthetic", "internal"}),
            distributions=frozenset(item.value for item in DistributionKind),
            maximum_records=1_000_000,
            maximum_bytes=1_073_741_824,
            reproducibility="canonical-data-digest",
        )

    def validate(self, contract: GenerationContract) -> None:
        for record in contract.records:
            for field in record.fields.values():
                distribution = field.distribution
                if distribution is None:
                    continue
                if distribution.kind in {DistributionKind.NORMAL, DistributionKind.UNIFORM} and (
                    field.type not in {FieldType.INTEGER, FieldType.NUMBER}
                ):
                    raise SyntheticDataError(
                        ErrorCode.CONTRACT_INVALID,
                        "numeric distributions require integer or number fields",
                    )

    @staticmethod
    def _random(seed: str, record_type: str, field_name: str, index: int) -> random.Random:
        digest = hashlib.sha256(f"{seed}\0{record_type}\0{field_name}\0{index}".encode()).digest()
        return random.Random(int.from_bytes(digest[:16], "big"))

    def _value(
        self,
        *,
        seed: str,
        record_type: str,
        field_name: str,
        index: int,
        spec: FieldSpec,
    ) -> Any:
        randomizer = self._random(seed, record_type, field_name, index)
        distribution = spec.distribution
        if spec.nullable and randomizer.random() < 0.05:
            return None
        if distribution is None:
            seed_digest = hashlib.sha256(f"{seed}\0{record_type}\0{field_name}".encode()).digest()
            if spec.type is FieldType.STRING:
                seed_prefix = seed_digest.hex()[:12]
                return f"{record_type}_{field_name}_{seed_prefix}_{index:08d}"
            if spec.type is FieldType.INTEGER:
                return int.from_bytes(seed_digest[:4], "big") % 1_000_000_000 + index
            if spec.type is FieldType.NUMBER:
                return float(int.from_bytes(seed_digest[:4], "big") % 1_000_000_000 + index)
            if spec.type is FieldType.BOOLEAN:
                return bool((seed_digest[0] + index) % 2)
            if spec.type is FieldType.DATE:
                offset = int.from_bytes(seed_digest[:2], "big") % 3_650
                return (date(2000, 1, 1) + timedelta(days=offset + index)).isoformat()
            offset = int.from_bytes(seed_digest[:4], "big")
            return (
                datetime(2000, 1, 1, tzinfo=UTC) + timedelta(seconds=offset + index)
            ).isoformat()
        kind = distribution.kind
        if kind is DistributionKind.CONSTANT:
            return distribution.value
        if kind is DistributionKind.SEQUENCE:
            return (distribution.start or 0) + index * (distribution.step or 1)
        if kind is DistributionKind.CHOICE:
            return distribution.values[randomizer.randrange(len(distribution.values))]
        if kind is DistributionKind.UUID:
            material = f"{seed}:{record_type}:{field_name}:{index}".encode()
            raw = hashlib.sha256(material).digest()[:16]
            return str(uuid.UUID(bytes=raw, version=4))
        if kind is DistributionKind.UNIFORM:
            assert distribution.minimum is not None and distribution.maximum is not None
            value = randomizer.uniform(distribution.minimum, distribution.maximum)
            return round(value) if spec.type is FieldType.INTEGER else value
        if kind is DistributionKind.NORMAL:
            assert distribution.mean is not None and distribution.stddev is not None
            value = randomizer.normalvariate(distribution.mean, distribution.stddev)
            return round(value) if spec.type is FieldType.INTEGER else value
        raise AssertionError(f"unsupported distribution: {kind}")

    def generate(self, contract: GenerationContract, seed: str) -> GeneratedDataset:
        generated: dict[str, tuple[dict[str, Any], ...]] = {}
        for record_spec in contract.records:
            rows = []
            for index in range(record_spec.count):
                rows.append(
                    {
                        name: self._value(
                            seed=seed,
                            record_type=record_spec.record_type,
                            field_name=name,
                            index=index,
                            spec=field,
                        )
                        for name, field in sorted(record_spec.fields.items())
                    }
                )
            generated[record_spec.record_type] = tuple(rows)
        for relation in contract.relations:
            source_type, source_field = relation.from_field.split(".", 1)
            target_type, target_field = relation.to_field.split(".", 1)
            target_values = [row[target_field] for row in generated[target_type]]
            if not target_values and relation.required:
                raise SyntheticDataError(
                    ErrorCode.CONTRACT_INVALID,
                    "required relation has no target",
                )
            for index, row in enumerate(generated[source_type]):
                row[source_field] = (
                    target_values[index % len(target_values)] if target_values else None
                )
        return validate_generated_dataset(contract, generated)
