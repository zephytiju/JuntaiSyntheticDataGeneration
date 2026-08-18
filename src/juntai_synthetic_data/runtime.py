"""Production composition using released shared foundations and injected credentials."""

from __future__ import annotations

import os
import re
from pathlib import Path

import psycopg
from juntai.artifact import ArtifactClient, OrasOCITransport, mtls_channel_credentials
from juntai.observability import ObservabilityConfig, configure_observability
from juntai.usage import UsageReporter

from juntai_synthetic_data.jobs import SqlJobRepository
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.providers import DeterministicTabularProvider, ProviderRegistry
from juntai_synthetic_data.publication import ArtifactDatasetPublisher
from juntai_synthetic_data.quotas import InMemoryQuotaLedger, QuotaLimits
from juntai_synthetic_data.service import SyntheticDataService

_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"required environment variable is missing: {name}")
    return value


def _bytes(name: str) -> bytes:
    return Path(_required(name)).read_bytes()


def build_runtime_service() -> SyntheticDataService:
    worker_digest = _required("JUNTAI_WORKER_IMAGE_DIGEST")
    if not _DIGEST.fullmatch(worker_digest):
        raise RuntimeError("JUNTAI_WORKER_IMAGE_DIGEST must be an immutable sha256 digest")
    repository = SqlJobRepository(lambda: psycopg.connect(_required("JUNTAI_JOB_DATABASE_DSN")))
    credentials = mtls_channel_credentials(
        root_certificates=_bytes("JUNTAI_ARTIFACT_REGISTRY_CA"),
        private_key=_bytes("JUNTAI_ARTIFACT_REGISTRY_KEY"),
        certificate_chain=_bytes("JUNTAI_ARTIFACT_REGISTRY_CERT"),
    )
    oci = OrasOCITransport(
        registry=_required("JUNTAI_OCI_REGISTRY"),
        repository_prefix=_required("JUNTAI_OCI_REPOSITORY_PREFIX"),
    )
    artifact_client = ArtifactClient.connect(
        registry_target=_required("JUNTAI_ARTIFACT_REGISTRY_TARGET"),
        registry_credentials=credentials,
        oci=oci,
    )
    provider = DeterministicTabularProvider(worker_image_digest=worker_digest)
    telemetry = configure_observability(
        ObservabilityConfig(
            service_namespace="juntai",
            service_name="synthetic-data-generation",
            service_version="1.0.0",
            deployment_environment=os.getenv("JUNTAI_ENVIRONMENT", "production"),
            artifact_digest=os.getenv("JUNTAI_SERVICE_IMAGE_DIGEST"),
            otlp_endpoint=os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://127.0.0.1:4318"),
        )
    )
    return SyntheticDataService(
        repository=repository,
        providers=ProviderRegistry((provider,)),
        policy=DefaultPolicyEngine(),
        quotas=InMemoryQuotaLedger(QuotaLimits()),
        publisher=ArtifactDatasetPublisher(artifact_client),
        usage_reporter=UsageReporter.from_otel(telemetry.logger_provider),
        source_revision=_required("JUNTAI_SOURCE_REVISION"),
    )
