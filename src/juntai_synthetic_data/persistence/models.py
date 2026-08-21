"""Persistence protocol shared by the service and repository implementations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from juntai_synthetic_data.contracts.models import (
    CreateGenerationRequest,
    GenerationResult,
)
from juntai_synthetic_data.dataset import GeneratedDataset
from juntai_synthetic_data.providers import GeneratorProviderManifest


@dataclass(frozen=True)
class GenerationWrite:
    generation_id: str
    request: CreateGenerationRequest
    dataset: GeneratedDataset
    provider: GeneratorProviderManifest
    policy_digest: str


@dataclass(frozen=True)
class CommitOutcome:
    result: GenerationResult
    replayed: bool


class GenerationRepository(Protocol):
    def find_idempotent(self, tenant_id: str, idempotency_key: str) -> GenerationResult | None: ...

    def commit(
        self,
        tenant_id: str,
        idempotency_key: str,
        write: GenerationWrite,
    ) -> CommitOutcome: ...

    def get(self, tenant_id: str, generation_id: str) -> GenerationResult | None: ...

    def delete(self, tenant_id: str, generation_id: str) -> GenerationResult | None: ...
