"""Synchronous application service for generation, recovery, and deletion."""

from __future__ import annotations

import re
import time
import uuid
from collections.abc import Callable

from juntai_synthetic_data.contracts.models import (
    CreateGenerationRequest,
    GenerationResult,
    validate_idempotency_key,
)
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.persistence import CommitOutcome, GenerationRepository, GenerationWrite
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.providers import ProviderRegistry

_TENANT = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")


class SyntheticDataService:
    def __init__(
        self,
        *,
        repository: GenerationRepository,
        providers: ProviderRegistry,
        policy: DefaultPolicyEngine,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.repository = repository
        self.providers = providers
        self.policy = policy
        self.monotonic = monotonic

    @staticmethod
    def _tenant(value: str) -> str:
        if not _TENANT.fullmatch(value):
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "authenticated tenant identity is not a bounded opaque identifier",
            )
        return value

    @staticmethod
    def _idempotency(value: str) -> str:
        try:
            return validate_idempotency_key(value)
        except ValueError as error:
            raise SyntheticDataError(ErrorCode.CONTRACT_INVALID, str(error)) from error

    def create_generation(
        self,
        tenant_id: str,
        idempotency_key: str,
        request: CreateGenerationRequest,
    ) -> CommitOutcome:
        tenant_id = self._tenant(tenant_id)
        idempotency_key = self._idempotency(idempotency_key)
        existing = self.repository.find_idempotent(tenant_id, idempotency_key)
        if existing is not None:
            if existing.request_digest != request.digest:
                raise SyntheticDataError(
                    ErrorCode.IDEMPOTENCY_KEY_REUSED,
                    "Idempotency-Key was already used for different content",
                )
            return CommitOutcome(existing, True)
        decision = self.policy.evaluate(request)
        provider = self.providers.select(request)
        self.repository.validate_destinations(tenant_id, request.generation_contract)
        started = self.monotonic()
        dataset = provider.generate(request.generation_contract, request.seed)
        if self.monotonic() - started > request.provider.requirements.maximum_runtime_seconds:
            raise SyntheticDataError(
                ErrorCode.DEPENDENCY_UNAVAILABLE,
                "in-process generation exceeded the synchronous deadline",
                retryable=True,
            )
        return self.repository.commit(
            tenant_id,
            idempotency_key,
            GenerationWrite(
                generation_id=f"gen_{uuid.uuid4().hex}",
                request=request,
                dataset=dataset,
                provider=provider.manifest,
                policy_digest=decision.digest,
            ),
        )

    def get_generation(self, tenant_id: str, generation_id: str) -> GenerationResult:
        result = self.repository.get(self._tenant(tenant_id), generation_id)
        if result is None:
            raise SyntheticDataError(ErrorCode.GENERATION_NOT_FOUND, "generation not found")
        return result

    def delete_generation(self, tenant_id: str, generation_id: str) -> GenerationResult:
        result = self.repository.delete(self._tenant(tenant_id), generation_id)
        if result is None:
            raise SyntheticDataError(ErrorCode.GENERATION_NOT_FOUND, "generation not found")
        return result
