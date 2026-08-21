from __future__ import annotations

from typing import Any

import pytest

from juntai_synthetic_data.contracts.models import CreateGenerationRequest, FieldType
from juntai_synthetic_data.persistence import (
    ForeignKeyDefinition,
    InMemoryGenerationRepository,
    TableDefinition,
)
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.providers import DeterministicTabularProvider, ProviderRegistry
from juntai_synthetic_data.service import SyntheticDataService


def request_data(*, max_bytes: int = 1_000_000) -> dict[str, Any]:
    return {
        "contract_version": "juntai.synthetic-data.request/v1",
        "generation_contract": {
            "contract_version": "juntai.synthetic-data.contract/v1",
            "records": [
                {
                    "record_type": "site",
                    "count": 3,
                    "destination": {
                        "schema": "axiom_preview",
                        "table": "site",
                        "columns": {"site_id": "site_id", "name": "display_name"},
                        "key_fields": ["site_id"],
                    },
                    "fields": {
                        "site_id": {
                            "type": "string",
                            "unique": True,
                            "distribution": {"kind": "uuid"},
                        },
                        "name": {"type": "string"},
                    },
                },
                {
                    "record_type": "asset",
                    "count": 6,
                    "destination": {
                        "schema": "lattice_preview",
                        "table": "asset",
                        "columns": {
                            "asset_id": "asset_id",
                            "site_id": "site_id",
                            "reading": "reading",
                        },
                        "key_fields": ["asset_id"],
                    },
                    "fields": {
                        "asset_id": {"type": "string", "unique": True},
                        "site_id": {"type": "string"},
                        "reading": {
                            "type": "number",
                            "distribution": {"kind": "normal", "mean": 65, "stddev": 12},
                        },
                    },
                },
            ],
            "relations": [{"from": "asset.site_id", "to": "site.site_id", "required": True}],
            "bounds": {"max_records": 9, "max_bytes": max_bytes},
        },
        "seed": "acceptance-seed-1",
        "provider": {"class": "tabular", "requirements": {"deterministic": True}},
        "policy": {"data_classification": "synthetic"},
    }


def catalog() -> dict[tuple[str, str], TableDefinition]:
    return {
        ("axiom_preview", "site"): TableDefinition(
            columns={"site_id": FieldType.STRING, "display_name": FieldType.STRING},
            unique_keys=(("site_id",),),
            required_columns=frozenset({"site_id", "display_name"}),
        ),
        ("lattice_preview", "asset"): TableDefinition(
            columns={
                "asset_id": FieldType.STRING,
                "site_id": FieldType.STRING,
                "reading": FieldType.NUMBER,
            },
            unique_keys=(("asset_id",),),
            required_columns=frozenset({"asset_id", "site_id", "reading"}),
            foreign_keys=(
                ForeignKeyDefinition(
                    columns=("site_id",),
                    target_schema="axiom_preview",
                    target_table="site",
                    target_columns=("site_id",),
                ),
            ),
        ),
    }


def make_repository() -> InMemoryGenerationRepository:
    return InMemoryGenerationRepository(catalog())


def make_service(
    *,
    repository: InMemoryGenerationRepository | None = None,
    monotonic=lambda: 0.0,
) -> SyntheticDataService:
    provider = DeterministicTabularProvider()
    return SyntheticDataService(
        repository=repository or make_repository(),
        providers=ProviderRegistry((provider,)),
        policy=DefaultPolicyEngine(),
        monotonic=monotonic,
    )


@pytest.fixture
def sample_request() -> CreateGenerationRequest:
    return CreateGenerationRequest.model_validate(request_data())
