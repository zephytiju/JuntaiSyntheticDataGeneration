"""Provider compatibility and generation protocol."""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Protocol

from juntai_synthetic_data.contracts.models import GenerationContract
from juntai_synthetic_data.dataset import BoundedDatasetSink, DatasetOutput
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


@dataclass(frozen=True)
class GeneratorProviderManifest:
    provider_id: str
    version: str
    provider_class: str
    contract_versions: frozenset[str]
    generation_modes: frozenset[str]
    deterministic_seed: bool
    privacy_classes: frozenset[str]
    formats: frozenset[str]
    distributions: frozenset[str]
    maximum_records: int
    maximum_bytes: int
    network_policy: str
    worker_image_digest: str
    reproducibility: str
    model_identity: str | None = None
    model_version: str | None = None

    def __post_init__(self) -> None:
        if self.network_policy != "deny-all":
            raise ValueError("providers must declare deny-all network policy")
        if not self.worker_image_digest.startswith("sha256:"):
            raise ValueError("worker image must be digest-pinned")


@dataclass(frozen=True)
class GenerationExecutionContext:
    job_id: str
    tenant_id: str
    cancellation_requested: Callable[[], bool]
    deadline_seconds: int
    started_at: float = field(default_factory=time.monotonic)

    def checkpoint(self) -> None:
        if self.cancellation_requested():
            raise InterruptedError("generation cancelled")
        if time.monotonic() - self.started_at > self.deadline_seconds:
            raise SyntheticDataError(
                ErrorCode.DEPENDENCY_DEADLINE,
                "generation deadline exceeded",
                retryable=True,
            )


class GeneratorProvider(Protocol):
    manifest: GeneratorProviderManifest

    def validate(self, contract: GenerationContract) -> None: ...

    def generate(
        self,
        contract: GenerationContract,
        seed: str,
        output: BoundedDatasetSink,
        context: GenerationExecutionContext,
    ) -> DatasetOutput: ...
