from __future__ import annotations

import pytest
from conftest import request_data

from juntai_synthetic_data.contracts.models import CreateGenerationRequest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.providers import DeterministicTabularProvider


def generate(request: CreateGenerationRequest, seed: str):
    return DeterministicTabularProvider().generate(request.generation_contract, seed)


def test_same_seed_reproduces_exact_data_digest(
    sample_request: CreateGenerationRequest,
) -> None:
    first = generate(sample_request, "seed")
    second = generate(sample_request, "seed")
    assert first.data_digest == second.data_digest
    assert first.records == second.records


def test_different_seed_changes_dataset(sample_request: CreateGenerationRequest) -> None:
    assert (
        generate(sample_request, "seed-a").data_digest
        != generate(sample_request, "seed-b").data_digest
    )


def test_default_unique_strings_change_with_seed(
    sample_request: CreateGenerationRequest,
) -> None:
    first = generate(sample_request, "seed-a").records["asset"]
    second = generate(sample_request, "seed-b").records["asset"]

    assert {row["asset_id"] for row in first}.isdisjoint({row["asset_id"] for row in second})


def test_referential_constraints_are_satisfied(
    sample_request: CreateGenerationRequest,
) -> None:
    output = generate(sample_request, "relations")
    sites = {row["site_id"] for row in output.records["site"]}
    assert output.records["asset"]
    assert all(row["site_id"] in sites for row in output.records["asset"])


def test_hard_output_limit_stops_generation() -> None:
    request = CreateGenerationRequest.model_validate(request_data(max_bytes=10))
    with pytest.raises(SyntheticDataError) as captured:
        generate(request, "bounded")
    assert captured.value.code is ErrorCode.OUTPUT_LIMIT_EXCEEDED


def test_provider_has_no_worker_or_network_contract() -> None:
    manifest = DeterministicTabularProvider().manifest
    assert not hasattr(manifest, "worker_image_digest")
    assert not hasattr(manifest, "network_policy")
