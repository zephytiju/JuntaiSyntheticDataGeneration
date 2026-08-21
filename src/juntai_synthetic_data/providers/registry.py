"""Deterministic in-process provider selection."""

from __future__ import annotations

from juntai_synthetic_data.contracts.models import CreateGenerationRequest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

from .base import GeneratorProvider


class ProviderRegistry:
    def __init__(self, providers: tuple[GeneratorProvider, ...]) -> None:
        self._providers = tuple(sorted(providers, key=lambda item: item.manifest.provider_id))
        if len({provider.manifest.provider_id for provider in providers}) != len(providers):
            raise ValueError("provider IDs must be unique")

    def select(self, request: CreateGenerationRequest) -> GeneratorProvider:
        contract = request.generation_contract
        requirements = request.provider.requirements
        for provider in self._providers:
            manifest = provider.manifest
            distributions = {
                field.distribution.kind.value
                for record in contract.records
                for field in record.fields.values()
                if field.distribution is not None
            }
            compatible = (
                manifest.provider_class == request.provider.provider_class
                and contract.contract_version in manifest.contract_versions
                and distributions <= manifest.distributions
                and contract.bounds.max_records <= manifest.maximum_records
                and contract.bounds.max_bytes <= manifest.maximum_bytes
                and request.policy.data_classification in manifest.privacy_classes
                and set(requirements.modes) <= manifest.generation_modes
                and manifest.deterministic_seed
            )
            if compatible:
                provider.validate(contract)
                return provider
        raise SyntheticDataError(
            ErrorCode.PROVIDER_UNSUPPORTED,
            "no allowed in-process provider satisfies the declared request",
        )
