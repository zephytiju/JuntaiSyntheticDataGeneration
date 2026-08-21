"""Synchronous V1 generation API."""

from __future__ import annotations

from fastapi import HTTPException, Request, Response
from juntai.sdk.fuse_api import EndpointGroup, FuseServer, ProfileSelectionRequest, select_profile

from juntai_synthetic_data.contracts.models import CreateGenerationRequest, GenerationResult
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.service import SyntheticDataService

from .auth import RequestAuthorizer, UnconfiguredAuthorizer

_HTTP_STATUS = {
    ErrorCode.CONTRACT_INVALID: 422,
    ErrorCode.PROVIDER_UNSUPPORTED: 422,
    ErrorCode.POLICY_DENIED: 403,
    ErrorCode.OUTPUT_LIMIT_EXCEEDED: 422,
    ErrorCode.DESTINATION_INVALID: 422,
    ErrorCode.DESTINATION_FORBIDDEN: 403,
    ErrorCode.DESTINATION_CONFLICT: 409,
    ErrorCode.IDEMPOTENCY_KEY_REUSED: 409,
    ErrorCode.GENERATION_NOT_FOUND: 404,
    ErrorCode.DELETE_CONFLICT: 409,
    ErrorCode.DEPENDENCY_UNAVAILABLE: 503,
}


def _raise_http(error: SyntheticDataError) -> None:
    raise HTTPException(status_code=_HTTP_STATUS[error.code], detail=error.to_dict())


def build_generation_group(
    service: SyntheticDataService,
    authorizer: RequestAuthorizer | None = None,
) -> EndpointGroup:
    authorizer = authorizer or UnconfiguredAuthorizer()
    generations = EndpointGroup("synthetic_data_generations", tags=["Synthetic Data Generations"])
    create_http = {
        "parameters": {
            "idempotency_key": {"in": "header", "name": "Idempotency-Key"},
        },
        "security": [{"bearerAuth": []}],
    }
    secured = {"security": [{"bearerAuth": []}]}

    @generations.endpoint(
        path="/v1/generations",
        method="POST",
        protocols=["http"],
        request_model=CreateGenerationRequest,
        response_model=GenerationResult,
        operation_id="syntheticData.createGeneration",
        summary="Synchronously generate and commit preview application data",
        http=create_http,
    )
    async def create_generation(
        body: CreateGenerationRequest,
        idempotency_key: str,
        request: Request,
        response: Response,
    ) -> GenerationResult:
        try:
            tenant_id = await authorizer.authorize(request, action="create")
            outcome = service.create_generation(tenant_id, idempotency_key, body)
            response.status_code = 200 if outcome.replayed else 201
            return outcome.result
        except SyntheticDataError as error:
            _raise_http(error)

    @generations.endpoint(
        path="/v1/generations/{generation_id}",
        method="GET",
        protocols=["http"],
        response_model=GenerationResult,
        operation_id="syntheticData.getGeneration",
        summary="Recover committed or deleted generation metadata",
        http=secured,
    )
    async def get_generation(generation_id: str, request: Request) -> GenerationResult:
        try:
            tenant_id = await authorizer.authorize(
                request, action="read", generation_id=generation_id
            )
            return service.get_generation(tenant_id, generation_id)
        except SyntheticDataError as error:
            _raise_http(error)

    @generations.endpoint(
        path="/v1/generations/{generation_id}",
        method="DELETE",
        protocols=["http"],
        response_model=GenerationResult,
        operation_id="syntheticData.deleteGeneration",
        summary="Delete exactly the application rows written by a generation",
        http=secured,
    )
    async def delete_generation(generation_id: str, request: Request) -> GenerationResult:
        try:
            tenant_id = await authorizer.authorize(
                request, action="delete", generation_id=generation_id
            )
            return service.delete_generation(tenant_id, generation_id)
        except SyntheticDataError as error:
            _raise_http(error)

    return generations


def build_server(
    service: SyntheticDataService,
    *,
    authorizer: RequestAuthorizer | None = None,
    enable_runtime: bool = True,
) -> FuseServer:
    profile = select_profile(
        ProfileSelectionRequest(
            identifier="juntai.fuse.profile.http",
            version="1.0.0",
            purpose="runtime" if enable_runtime else "artifact",
            framework_version="2.0.0",
            descriptor_version="juntai.fuse/v1alpha1",
        )
    )
    server = FuseServer(title="Juntai Synthetic Data Generation", version="1.3.0", profile=profile)
    server.register(build_generation_group(service, authorizer))
    if enable_runtime:
        server.enable_selected_adapter()
    return server
