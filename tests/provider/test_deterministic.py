from __future__ import annotations

import json
import time

import pytest
from conftest import IMAGE_DIGEST, request_data

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.dataset import BoundedDatasetSink
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.providers import DeterministicTabularProvider, GenerationExecutionContext


def generate(request: CreateJobRequest, seed: str):
    provider = DeterministicTabularProvider(worker_image_digest=IMAGE_DIGEST)
    with BoundedDatasetSink(request.generation_contract) as sink:
        return provider.generate(
            request.generation_contract,
            seed,
            sink,
            GenerationExecutionContext("job", "tenant", lambda: False, 30),
        )


def test_same_seed_reproduces_exact_logical_dataset(sample_request: CreateJobRequest) -> None:
    first = generate(sample_request, "seed")
    second = generate(sample_request, "seed")
    assert first.logical_digest == second.logical_digest
    assert [shard.digest for shard in first.shards] == [shard.digest for shard in second.shards]


def test_different_seed_changes_dataset(sample_request: CreateJobRequest) -> None:
    assert (
        generate(sample_request, "seed-a").logical_digest
        != generate(sample_request, "seed-b").logical_digest
    )


def test_referential_constraints_are_satisfied(sample_request: CreateJobRequest) -> None:
    output = generate(sample_request, "relations")
    rows = [json.loads(line) for line in output.shards[0].data.splitlines()]
    sites = {row["record"]["site_id"] for row in rows if row["record_type"] == "site"}
    assets = [row for row in rows if row["record_type"] == "asset"]
    assert assets
    assert all(row["record"]["site_id"] in sites for row in assets)


def test_hard_output_limit_stops_generation() -> None:
    request = CreateJobRequest.model_validate(request_data(max_bytes=10))
    with pytest.raises(SyntheticDataError) as captured:
        generate(request, "bounded")
    assert captured.value.code is ErrorCode.OUTPUT_LIMIT_EXCEEDED


def test_generation_cancellation_checkpoint(sample_request: CreateJobRequest) -> None:
    provider = DeterministicTabularProvider(worker_image_digest=IMAGE_DIGEST)
    with (
        BoundedDatasetSink(sample_request.generation_contract) as sink,
        pytest.raises(InterruptedError),
    ):
        provider.generate(
            sample_request.generation_contract,
            "cancel",
            sink,
            GenerationExecutionContext("job", "tenant", lambda: True, 30),
        )


def test_generation_deadline_fails_stably(sample_request: CreateJobRequest) -> None:
    context = GenerationExecutionContext(
        "job",
        "tenant",
        lambda: False,
        1,
        started_at=time.monotonic() - 2,
    )
    with pytest.raises(SyntheticDataError) as captured:
        context.checkpoint()
    assert captured.value.code is ErrorCode.DEPENDENCY_DEADLINE
