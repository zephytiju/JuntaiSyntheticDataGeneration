"""Production SWP worker composition without SQL, queue, API, or Kubernetes imports."""

from __future__ import annotations

import os
import re
from pathlib import Path

import grpc
from juntai.artifact import ArtifactClient, OrasOCITransport, mtls_channel_credentials

from juntai_synthetic_data.execution import (
    ArtifactExecutionEvidencePublisher,
    ArtifactExecutionInputResolver,
    SyntheticWorkerEngine,
)
from juntai_synthetic_data.providers import DeterministicTabularProvider, ProviderRegistry
from juntai_synthetic_data.publication import ArtifactDatasetPublisher
from juntai_synthetic_data.worker import SocketWorker
from juntai_synthetic_data.worker_protocol import WorkloadIdentity

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
_COMMIT = re.compile(r"^[0-9a-f]{40}$")


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"required worker environment variable is missing: {name}")
    return value


def _bytes(name: str) -> bytes:
    return Path(_required(name)).read_bytes()


def build_worker() -> SocketWorker:
    worker_digest = _required("JUNTAI_WORKER_IMAGE_DIGEST")
    source_revision = _required("JUNTAI_SOURCE_REVISION")
    if not _DIGEST.fullmatch(worker_digest):
        raise RuntimeError("JUNTAI_WORKER_IMAGE_DIGEST must be an immutable sha256 digest")
    if not _COMMIT.fullmatch(source_revision):
        raise RuntimeError("JUNTAI_SOURCE_REVISION must be an exact lowercase Git commit")
    credentials = mtls_channel_credentials(
        root_certificates=_bytes("JUNTAI_ARTIFACT_REGISTRY_CA"),
        private_key=_bytes("JUNTAI_ARTIFACT_REGISTRY_KEY"),
        certificate_chain=_bytes("JUNTAI_ARTIFACT_REGISTRY_CERT"),
    )
    artifact_client = ArtifactClient.connect(
        registry_target=_required("JUNTAI_ARTIFACT_REGISTRY_TARGET"),
        registry_credentials=credentials,
        oci=OrasOCITransport(
            registry=_required("JUNTAI_OCI_REGISTRY"),
            repository_prefix=_required("JUNTAI_OCI_REPOSITORY_PREFIX"),
        ),
    )
    token = _bytes("JUNTAI_ARTIFACT_WORKLOAD_TOKEN_FILE").decode().strip()
    if not token:
        raise RuntimeError("Artifact workload token file is empty")
    caller_credentials = grpc.access_token_call_credentials(token)
    engine = SyntheticWorkerEngine(
        inputs=ArtifactExecutionInputResolver(artifact_client, caller_credentials),
        providers=ProviderRegistry(
            (DeterministicTabularProvider(worker_image_digest=worker_digest),)
        ),
        publisher=ArtifactDatasetPublisher(artifact_client),
        evidence=ArtifactExecutionEvidencePublisher(
            artifact_client, producer_build_id=source_revision
        ),
        source_revision=source_revision,
    )
    return SocketWorker(
        engine,
        workload=WorkloadIdentity(
            namespace=_required("JUNTAI_WORKLOAD_NAMESPACE"),
            serviceAccount=_required("JUNTAI_WORKLOAD_SERVICE_ACCOUNT"),
        ),
    )
