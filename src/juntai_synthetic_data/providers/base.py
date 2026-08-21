"""In-process provider compatibility and generation protocol."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from juntai_synthetic_data.contracts.models import GenerationContract
from juntai_synthetic_data.dataset import GeneratedDataset


@dataclass(frozen=True)
class GeneratorProviderManifest:
    provider_id: str
    version: str
    provider_class: str
    contract_versions: frozenset[str]
    generation_modes: frozenset[str]
    deterministic_seed: bool
    privacy_classes: frozenset[str]
    distributions: frozenset[str]
    maximum_records: int
    maximum_bytes: int
    reproducibility: str
    model_identity: str | None = None
    model_version: str | None = None


class GeneratorProvider(Protocol):
    manifest: GeneratorProviderManifest

    def validate(self, contract: GenerationContract) -> None: ...

    def generate(self, contract: GenerationContract, seed: str) -> GeneratedDataset: ...
