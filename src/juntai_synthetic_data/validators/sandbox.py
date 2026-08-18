"""Side-effect-free validator handoff to an injected no-network sandbox executor."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from juntai_synthetic_data.contracts.models import ValidatorDescriptor, canonical_digest
from juntai_synthetic_data.dataset import DatasetOutput
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


@dataclass(frozen=True)
class SandboxPolicy:
    network: str = "deny-all"
    root_filesystem: str = "read-only"
    run_as_non_root: bool = True
    allow_process_spawn: bool = False
    allow_background_threads: bool = False
    allow_database_connections: bool = False
    allow_dataset_mutation: bool = False


@dataclass(frozen=True)
class SandboxExecutionRequest:
    descriptor: ValidatorDescriptor
    dataset: DatasetOutput
    policy: SandboxPolicy
    artifact_layers: tuple[bytes, ...]


@dataclass(frozen=True)
class SandboxExecutionResult:
    passed: bool
    findings: tuple[str, ...] = ()
    attempted_side_effects: tuple[str, ...] = ()


@dataclass(frozen=True)
class ValidatorEvidence:
    passed: bool
    findings: tuple[str, ...]
    digest: str


class SandboxExecutor(Protocol):
    def execute(self, request: SandboxExecutionRequest) -> SandboxExecutionResult: ...


class ValidatorArtifactResolver(Protocol):
    def resolve_exact(self, descriptor: ValidatorDescriptor) -> tuple[bytes, ...]: ...


class ValidatorSandbox:
    def __init__(self, executor: SandboxExecutor, resolver: ValidatorArtifactResolver) -> None:
        self.executor = executor
        self.resolver = resolver

    def validate(
        self, descriptor: ValidatorDescriptor, dataset: DatasetOutput
    ) -> ValidatorEvidence:
        policy = SandboxPolicy()
        artifact_layers = self.resolver.resolve_exact(descriptor)
        if not artifact_layers:
            raise SyntheticDataError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "validator Artifact contains no executable layer",
                retryable=True,
            )
        result = self.executor.execute(
            SandboxExecutionRequest(descriptor, dataset, policy, artifact_layers)
        )
        if result.attempted_side_effects:
            raise SyntheticDataError(
                ErrorCode.SANDBOX_VIOLATION,
                "validator attempted a prohibited side effect",
                details={"violations": ",".join(result.attempted_side_effects[:20])},
            )
        findings = tuple(item[:500] for item in result.findings[:100])
        evidence = {"passed": result.passed, "findings": findings, "validator": descriptor.digest}
        if not result.passed:
            raise SyntheticDataError(
                ErrorCode.VALIDATOR_FAILED,
                "validator rejected the generated dataset",
                details={"finding_count": len(findings)},
            )
        return ValidatorEvidence(True, findings, canonical_digest(evidence))
