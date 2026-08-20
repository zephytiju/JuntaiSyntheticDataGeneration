"""Production Juntai IAM composition over an injected immutable policy snapshot."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from juntai.iam import (
    JUNTAI_POLICY_MODEL_V1,
    CasdoorAccessTokenVerifier,
    CasdoorPolicyEvaluator,
    GroupingRecord,
    IamMiddleware,
    PolicyEffect,
    PolicyRecord,
    PolicySnapshot,
)

from juntai_synthetic_data.api.auth import JuntaiIamAuthorizer
from juntai_synthetic_data.iam_contract import validate_iam_runtime


class ImmutableFilePolicySource:
    """Reads a deployment-mounted snapshot; it does not own or mutate policy."""

    def __init__(self, path: str) -> None:
        self.path = Path(path)

    def load(self, tenant_id: str) -> PolicySnapshot:
        document = json.loads(self.path.read_text())
        raw: dict[str, Any] = document.get("tenants", {}).get(tenant_id, document)
        if raw.get("tenant_id", tenant_id) != tenant_id:
            raise ValueError("policy snapshot tenant does not match authenticated tenant")
        return PolicySnapshot(
            revision=str(raw["revision"]),
            policies=tuple(
                PolicyRecord(
                    policy_id=str(item["policy_id"]),
                    subject_or_role=str(item["subject_or_role"]),
                    tenant=tenant_id,
                    resource_pattern=str(item["resource_pattern"]),
                    action_pattern=str(item["action_pattern"]),
                    field_pattern=str(item.get("field_pattern", "**")),
                    effect=PolicyEffect(str(item["effect"])),
                )
                for item in raw.get("policies", ())
            ),
            groupings=tuple(
                GroupingRecord(
                    subject=str(item["subject"]),
                    role=str(item["role"]),
                    tenant=tenant_id,
                )
                for item in raw.get("groupings", ())
            ),
        )


def build_runtime_authorizer(
    *,
    issuer: str,
    audiences: tuple[str, ...],
    policy_snapshot_path: str,
    discovery_url: str | None = None,
) -> JuntaiIamAuthorizer:
    validate_iam_runtime()
    verifier = CasdoorAccessTokenVerifier(
        issuer=issuer,
        audiences=audiences,
        required_scopes=("synthetic-data:jobs",),
        discovery_url=discovery_url,
        required_token_type="access-token",
    )
    evaluator = CasdoorPolicyEvaluator(
        model=JUNTAI_POLICY_MODEL_V1,
        policy_source=ImmutableFilePolicySource(policy_snapshot_path),
    )
    return JuntaiIamAuthorizer(IamMiddleware(verifier=verifier, evaluator=evaluator))
