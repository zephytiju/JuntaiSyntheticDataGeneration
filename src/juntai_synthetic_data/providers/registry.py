"""Deterministic provider compatibility and selection."""

from __future__ import annotations

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

from .base import GeneratorProvider


class ProviderRegistry:
    def __init__(self, providers: tuple[GeneratorProvider, ...]) -> None:
        self._providers = tuple(sorted(providers, key=lambda item: item.manifest.provider_id))
        if len({provider.manifest.provider_id for provider in providers}) != len(providers):
            raise ValueError("provider IDs must be unique")

    def select(self, request: CreateJobRequest) -> GeneratorProvider:
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
                and contract.output.format in manifest.formats
                and distributions <= manifest.distributions
                and contract.bounds.max_records <= manifest.maximum_records
                and contract.bounds.max_bytes <= manifest.maximum_bytes
                and request.policy.data_classification in manifest.privacy_classes
                and set(requirements.modes) <= manifest.generation_modes
                and (not requirements.deterministic or manifest.deterministic_seed)
            )
            if compatible:
                provider.validate(contract)
                return provider
        code = (
            ErrorCode.DETERMINISTIC_SEED_INCOMPATIBLE
            if requirements.deterministic
            else ErrorCode.PROVIDER_UNSUPPORTED
        )
        raise SyntheticDataError(code, "no allowed provider satisfies the declared request")
