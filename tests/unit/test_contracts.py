from __future__ import annotations

import copy
import json

import pytest
from conftest import request_data
from pydantic import ValidationError

from juntai_synthetic_data.contracts.models import CreateGenerationRequest, Distribution


def test_canonical_request_digest_is_stable() -> None:
    first = CreateGenerationRequest.model_validate(request_data())
    second = CreateGenerationRequest.model_validate(copy.deepcopy(request_data()))
    assert first.digest == second.digest
    assert first.generation_contract.digest == second.generation_contract.digest


def test_request_alias_json_round_trips_for_sql_persistence() -> None:
    request = CreateGenerationRequest.model_validate(request_data())
    persisted = request.model_dump_json(exclude_none=True, by_alias=True)
    document = json.loads(persisted)

    assert document["provider"]["class"] == "tabular"
    assert document["generation_contract"]["records"][0]["destination"]["schema"] == (
        "axiom_preview"
    )
    assert CreateGenerationRequest.model_validate_json(persisted) == request


def test_unknown_semantic_fields_are_rejected() -> None:
    data = request_data()
    data["generation_contract"]["records"][0]["ontology"] = "forbidden"
    with pytest.raises(ValidationError, match="Extra inputs"):
        CreateGenerationRequest.model_validate(data)


def test_relation_target_must_be_unique() -> None:
    data = request_data()
    data["generation_contract"]["records"][0]["fields"]["site_id"]["unique"] = False
    with pytest.raises(ValidationError, match="target field must be unique"):
        CreateGenerationRequest.model_validate(data)


def test_record_counts_must_fit_global_bound() -> None:
    data = request_data()
    data["generation_contract"]["bounds"]["max_records"] = 8
    with pytest.raises(ValidationError, match="exceed max_records"):
        CreateGenerationRequest.model_validate(data)


@pytest.mark.parametrize(
    "distribution",
    [
        {"kind": "choice", "values": []},
        {"kind": "uniform", "minimum": 2, "maximum": 1},
        {"kind": "normal", "mean": 0},
        {"kind": "constant"},
        {"kind": "sequence", "step": 0},
    ],
)
def test_invalid_distribution_shapes_fail(distribution: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Distribution.model_validate(distribution)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("dsn", "postgresql://secret"),
        ("host", "database.internal"),
        ("database", "platform"),
        ("sql", "TRUNCATE TABLE anything"),
        ("tenant_id", "spoofed"),
    ],
)
def test_request_rejects_connection_sql_and_tenant_fields(field: str, value: str) -> None:
    data = request_data()
    data[field] = value
    with pytest.raises(ValidationError):
        CreateGenerationRequest.model_validate(data)


def test_destination_requires_complete_one_to_one_field_mapping() -> None:
    data = request_data()
    del data["generation_contract"]["records"][0]["destination"]["columns"]["name"]
    with pytest.raises(ValidationError, match="every generated field"):
        CreateGenerationRequest.model_validate(data)


def test_destination_key_must_be_generated_mapped_and_non_nullable() -> None:
    data = request_data()
    data["generation_contract"]["records"][0]["destination"]["key_fields"] = ["missing"]
    with pytest.raises(ValidationError, match="key_fields"):
        CreateGenerationRequest.model_validate(data)


def test_destination_identifiers_are_bounded_authoritative_strings() -> None:
    data = request_data()
    destination = data["generation_contract"]["records"][0]["destination"]
    destination["schema"] = "Axiom Preview"
    destination["table"] = 'Site "Records"'
    destination["columns"]["site_id"] = "Site ID"

    request = CreateGenerationRequest.model_validate(data)

    parsed = request.generation_contract.records[0].destination
    assert parsed.schema_name == "Axiom Preview"
    assert parsed.table == 'Site "Records"'
    assert parsed.columns["site_id"] == "Site ID"


def test_relation_cycles_are_rejected() -> None:
    data = request_data()
    data["generation_contract"]["records"][1]["fields"]["asset_id"]["type"] = "string"
    data["generation_contract"]["records"][0]["fields"]["asset_id"] = {"type": "string"}
    data["generation_contract"]["records"][0]["destination"]["columns"]["asset_id"] = "asset_id"
    data["generation_contract"]["records"][0]["fields"]["site_id"]["unique"] = True
    data["generation_contract"]["records"][1]["fields"]["asset_id"]["unique"] = True
    data["generation_contract"]["relations"].append(
        {"from": "site.asset_id", "to": "asset.asset_id", "required": True}
    )
    with pytest.raises(ValidationError, match="acyclic"):
        CreateGenerationRequest.model_validate(data)
