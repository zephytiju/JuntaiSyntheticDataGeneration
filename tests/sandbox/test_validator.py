from __future__ import annotations

import pytest
from conftest import IMAGE_DIGEST, ExactResolver, PassingExecutor, request_data

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.dataset import BoundedDatasetSink
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.providers import DeterministicTabularProvider, GenerationExecutionContext
from juntai_synthetic_data.validators import (
    SandboxExecutionRequest,
    SandboxExecutionResult,
    ValidatorSandbox,
)


def candidate():
    request = CreateJobRequest.model_validate(request_data(validator=True))
    provider = DeterministicTabularProvider(worker_image_digest=IMAGE_DIGEST)
    sink = BoundedDatasetSink(request.generation_contract)
    output = provider.generate(
        request.generation_contract,
        request.seed,
        sink,
        GenerationExecutionContext("job", "tenant", lambda: False, 30),
    )
    return request, sink, output


def test_validator_receives_strict_side_effect_free_policy() -> None:
    request, sink, output = candidate()
    executor = PassingExecutor()
    try:
        evidence = ValidatorSandbox(executor, ExactResolver()).validate(  # type: ignore[arg-type]
            request.validator, output
        )
    finally:
        sink.cleanup()
    policy = executor.requests[0].policy
    assert evidence.passed
    assert policy.network == "deny-all"
    assert policy.root_filesystem == "read-only"
    assert policy.run_as_non_root
    assert not policy.allow_process_spawn
    assert not policy.allow_background_threads
    assert not policy.allow_database_connections
    assert not policy.allow_dataset_mutation


class SideEffectExecutor:
    def execute(self, request: SandboxExecutionRequest) -> SandboxExecutionResult:
        del request
        return SandboxExecutionResult(True, attempted_side_effects=("network", "dataset-write"))


def test_validator_side_effect_fails_closed() -> None:
    request, sink, output = candidate()
    try:
        with pytest.raises(SyntheticDataError) as captured:
            ValidatorSandbox(SideEffectExecutor(), ExactResolver()).validate(  # type: ignore[arg-type]
                request.validator, output
            )
    finally:
        sink.cleanup()
    assert captured.value.code is ErrorCode.SANDBOX_VIOLATION


class RejectingExecutor:
    def execute(self, request: SandboxExecutionRequest) -> SandboxExecutionResult:
        del request
        return SandboxExecutionResult(False, findings=("record mismatch",))


def test_validator_rejection_publishes_nothing() -> None:
    request, sink, output = candidate()
    try:
        with pytest.raises(SyntheticDataError) as captured:
            ValidatorSandbox(RejectingExecutor(), ExactResolver()).validate(  # type: ignore[arg-type]
                request.validator, output
            )
    finally:
        sink.cleanup()
    assert captured.value.code is ErrorCode.VALIDATOR_FAILED
