from __future__ import annotations

from types import SimpleNamespace

import pytest
from starlette.requests import Request

from juntai_synthetic_data.api.auth import JuntaiIamAuthorizer


class CapturingIam:
    def __init__(self) -> None:
        self.authorization = None

    def require_human_or_delegated(self, request: Request):
        del request
        return SimpleNamespace(tenant_id="tenant-from-verified-identity")

    async def authorize(self, identity, request):
        del identity
        self.authorization = request


@pytest.mark.asyncio
async def test_authorizer_uses_verified_identity_tenant() -> None:
    iam = CapturingIam()
    authorizer = JuntaiIamAuthorizer(iam)  # type: ignore[arg-type]
    request = Request({"type": "http", "headers": []})
    tenant = await authorizer.authorize(request, action="read", job_id="job_123")
    assert tenant == "tenant-from-verified-identity"
    assert iam.authorization.tenant == tenant
    assert iam.authorization.resource == "synthetic-data/jobs/job_123"
