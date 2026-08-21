"""Released Juntai IAM authentication and authorization adapter."""

from __future__ import annotations

from typing import Protocol

from fastapi import Request
from juntai.iam import (
    AuthenticationError,
    AuthorizationDenied,
    AuthorizationRequest,
    IamMiddleware,
)

from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError


class RequestAuthorizer(Protocol):
    async def authorize(
        self, request: Request, *, action: str, generation_id: str | None = None
    ) -> str: ...


class JuntaiIamAuthorizer:
    def __init__(self, middleware: IamMiddleware) -> None:
        self.middleware = middleware

    async def authorize(
        self, request: Request, *, action: str, generation_id: str | None = None
    ) -> str:
        try:
            identity = self.middleware.require_human_or_delegated(request)
            resource = (
                "synthetic-data/generations"
                if generation_id is None
                else f"synthetic-data/generations/{generation_id}"
            )
            await self.middleware.authorize(
                identity,
                AuthorizationRequest(
                    tenant=identity.tenant_id,
                    resource=resource,
                    action=action,
                ),
            )
            return identity.tenant_id
        except AuthenticationError as exc:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED, "caller authentication failed"
            ) from exc
        except AuthorizationDenied as exc:
            raise SyntheticDataError(
                ErrorCode.POLICY_DENIED, "caller authorization denied"
            ) from exc


class UnconfiguredAuthorizer:
    async def authorize(
        self, request: Request, *, action: str, generation_id: str | None = None
    ) -> str:
        del request, action, generation_id
        raise SyntheticDataError(
            ErrorCode.DEPENDENCY_UNAVAILABLE,
            "Juntai IAM authorizer is not configured",
            retryable=True,
        )
