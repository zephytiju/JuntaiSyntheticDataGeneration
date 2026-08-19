"""One generic versioned FuseAPI job surface."""

from __future__ import annotations

from fastapi import HTTPException, Request
from juntai.sdk.fuse_api import EndpointGroup, FuseServer, ProfileSelectionRequest, select_profile

from juntai_synthetic_data.contracts.models import CreateJobRequest, JobResult, JobStatus
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.service import SyntheticDataService

from .auth import RequestAuthorizer, UnconfiguredAuthorizer

_HTTP_STATUS = {
    ErrorCode.CONTRACT_INVALID: 422,
    ErrorCode.PROVIDER_UNSUPPORTED: 422,
    ErrorCode.DETERMINISTIC_SEED_INCOMPATIBLE: 422,
    ErrorCode.POLICY_DENIED: 403,
    ErrorCode.QUOTA_EXCEEDED: 429,
    ErrorCode.IDEMPOTENCY_KEY_REUSED: 409,
    ErrorCode.JOB_NOT_FOUND: 404,
    ErrorCode.JOB_NOT_SUCCEEDED: 409,
    ErrorCode.CONCURRENCY_CONFLICT: 409,
}


def _raise_http(error: SyntheticDataError) -> None:
    raise HTTPException(status_code=_HTTP_STATUS.get(error.code, 503), detail=error.to_dict())


def build_job_group(
    service: SyntheticDataService,
    authorizer: RequestAuthorizer | None = None,
) -> EndpointGroup:
    authorizer = authorizer or UnconfiguredAuthorizer()
    jobs = EndpointGroup("synthetic_data_jobs", prefix="/v1/jobs", tags=["Synthetic Data Jobs"])
    headers = {
        "parameters": {
            "idempotency_key": {"in": "header", "name": "Idempotency-Key"},
        },
        "security": [{"bearerAuth": []}],
    }
    secured = {"security": [{"bearerAuth": []}]}

    @jobs.endpoint(
        path="",
        method="POST",
        protocols=["http"],
        request_model=CreateJobRequest,
        response_model=JobStatus,
        operation_id="syntheticData.createJob",
        summary="Create an asynchronous synthetic-data generation job",
        http=headers,
    )
    async def create_job(
        body: CreateJobRequest,
        idempotency_key: str,
        request: Request,
    ) -> JobStatus:
        try:
            tenant_id = await authorizer.authorize(request, action="create")
            return service.create_job(tenant_id, idempotency_key, body)
        except SyntheticDataError as error:
            _raise_http(error)

    @jobs.endpoint(
        path="/{job_id}",
        method="GET",
        protocols=["http"],
        response_model=JobStatus,
        operation_id="syntheticData.getJob",
        summary="Read bounded job status and evidence",
        http=secured,
    )
    async def get_job(job_id: str, request: Request) -> JobStatus:
        try:
            tenant_id = await authorizer.authorize(request, action="read", job_id=job_id)
            return service.status(service.get_job(tenant_id, job_id))
        except SyntheticDataError as error:
            _raise_http(error)

    @jobs.endpoint(
        path="/{job_id}:cancel",
        method="POST",
        protocols=["http"],
        response_model=JobStatus,
        operation_id="syntheticData.cancelJob",
        summary="Request best-effort job cancellation",
        http=secured,
    )
    async def cancel_job(job_id: str, request: Request) -> JobStatus:
        try:
            tenant_id = await authorizer.authorize(request, action="cancel", job_id=job_id)
            return service.cancel(tenant_id, job_id)
        except SyntheticDataError as error:
            _raise_http(error)

    @jobs.endpoint(
        path="/{job_id}/result",
        method="GET",
        protocols=["http"],
        response_model=JobResult,
        operation_id="syntheticData.getJobResult",
        summary="Read the exact immutable dataset Artifact result",
        http=secured,
    )
    async def get_job_result(job_id: str, request: Request) -> JobResult:
        try:
            tenant_id = await authorizer.authorize(request, action="read", job_id=job_id)
            return service.result(tenant_id, job_id)
        except SyntheticDataError as error:
            _raise_http(error)

    return jobs


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
    server = FuseServer(title="Juntai Synthetic Data Generation", version="1.2.0", profile=profile)
    server.register(build_job_group(service, authorizer))
    if enable_runtime:
        server.enable_selected_adapter()
    return server
