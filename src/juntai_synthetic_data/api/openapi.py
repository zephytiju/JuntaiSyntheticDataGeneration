"""Service-owned bearer IAM metadata for the routed public OpenAPI."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

IAM_AUDIENCE = "juntai.synthetic-data.api"

_AUTHORIZATION = {
    "syntheticData.createJob": ("synthetic-data/jobs", "create"),
    "syntheticData.getJob": ("synthetic-data/jobs/{job_id}", "read"),
    "syntheticData.cancelJob": ("synthetic-data/jobs/{job_id}", "cancel"),
    "syntheticData.getJobResult": ("synthetic-data/jobs/{job_id}", "read"),
}


def apply_bearer_security(document: dict[str, Any]) -> dict[str, Any]:
    """Attach reviewed service-owned IAM metadata without changing API semantics."""

    secured = deepcopy(document)
    components = secured.setdefault("components", {})
    schemes = components.setdefault("securitySchemes", {})
    schemes["bearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": f"Casdoor access token with exact audience {IAM_AUDIENCE}.",
    }
    observed: set[str] = set()
    for path in secured.get("paths", {}).values():
        for method, operation in path.items():
            if method.lower() not in {"get", "post", "put", "patch", "delete", "options", "head"}:
                continue
            operation_id = operation.get("operationId")
            if operation_id not in _AUTHORIZATION:
                raise ValueError(f"unexpected routed operation without IAM binding: {operation_id}")
            resource, action = _AUTHORIZATION[operation_id]
            operation["security"] = [{"bearerAuth": []}]
            operation["x-juntai-iam"] = {
                "audience": IAM_AUDIENCE,
                "resource": resource,
                "action": action,
                "tenantSource": "verified-casdoor-organization-or-delegated-workload",
                "defaultDeny": True,
            }
            observed.add(operation_id)
    if observed != set(_AUTHORIZATION):
        missing = sorted(set(_AUTHORIZATION) - observed)
        raise ValueError(f"routed OpenAPI is missing IAM-bound operations: {missing}")
    secured["security"] = [{"bearerAuth": []}]
    return secured
