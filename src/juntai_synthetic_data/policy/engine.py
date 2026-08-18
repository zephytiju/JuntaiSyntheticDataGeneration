"""Fail-closed privacy and safety policy with schema-only defaults."""

from __future__ import annotations

from dataclasses import dataclass

from juntai_synthetic_data.contracts.models import CreateJobRequest, canonical_digest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


@dataclass(frozen=True)
class PolicyDecision:
    policy_id: str
    policy_version: str
    digest: str


class DefaultPolicyEngine:
    policy_id = "juntai.synthetic-data.default"
    policy_version = "1.0.0"

    def evaluate(self, request: CreateJobRequest) -> PolicyDecision:
        policy = request.policy
        if policy.source_examples != "none" and not policy.authorization_reference:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "source-derived generation requires an authorization reference",
            )
        if policy.data_classification in {"confidential", "restricted"}:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "default policy permits synthetic and internal classifications only",
            )
        if request.validator is not None and not request.validator.deterministic:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED,
                "validator must declare deterministic behavior",
            )
        value = {
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "request_policy": policy.model_dump(mode="json", exclude_none=True),
        }
        return PolicyDecision(self.policy_id, self.policy_version, canonical_digest(value))
