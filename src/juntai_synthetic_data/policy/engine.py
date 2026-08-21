"""Fail-closed policy for bounded test-fleet synthetic generation."""

from __future__ import annotations

from dataclasses import dataclass

from juntai_synthetic_data.contracts.models import CreateGenerationRequest, canonical_digest


@dataclass(frozen=True)
class PolicyDecision:
    policy_id: str
    policy_version: str
    digest: str


class DefaultPolicyEngine:
    policy_id = "juntai.synthetic-data.test-fleet"
    policy_version = "1.0.0"

    def evaluate(self, request: CreateGenerationRequest) -> PolicyDecision:
        value = {
            "policy_id": self.policy_id,
            "policy_version": self.policy_version,
            "request_policy": request.policy.model_dump(mode="json"),
        }
        return PolicyDecision(self.policy_id, self.policy_version, canonical_digest(value))
