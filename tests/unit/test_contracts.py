from __future__ import annotations

import copy

import pytest
from conftest import request_data
from pydantic import ValidationError

from juntai_synthetic_data.contracts.models import CreateJobRequest, Distribution


def test_canonical_request_digest_is_stable() -> None:
    first = CreateJobRequest.model_validate(request_data())
    second = CreateJobRequest.model_validate(copy.deepcopy(request_data()))
    assert first.digest == second.digest
    assert first.generation_contract.digest == second.generation_contract.digest


def test_unknown_semantic_fields_are_rejected() -> None:
    data = request_data()
    data["generation_contract"]["records"][0]["ontology"] = "forbidden"
    with pytest.raises(ValidationError, match="Extra inputs"):
        CreateJobRequest.model_validate(data)


def test_relation_target_must_be_unique() -> None:
    data = request_data()
    data["generation_contract"]["records"][0]["fields"]["site_id"]["unique"] = False
    with pytest.raises(ValidationError, match="target field must be unique"):
        CreateJobRequest.model_validate(data)


def test_record_counts_must_fit_global_bound() -> None:
    data = request_data()
    data["generation_contract"]["bounds"]["max_records"] = 8
    with pytest.raises(ValidationError, match="exceed max_records"):
        CreateJobRequest.model_validate(data)


@pytest.mark.parametrize(
    "distribution",
    [
        {"kind": "choice", "values": []},
        {"kind": "uniform", "minimum": 2, "maximum": 1},
        {"kind": "normal", "mean": 0},
        {"kind": "constant"},
    ],
)
def test_invalid_distribution_shapes_fail(distribution: dict[str, object]) -> None:
    with pytest.raises(ValidationError):
        Distribution.model_validate(distribution)


def test_request_rejects_target_namespace_and_credentials() -> None:
    data = request_data()
    data["target_namespace"] = "not-accepted"
    with pytest.raises(ValidationError):
        CreateJobRequest.model_validate(data)
