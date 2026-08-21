"""Production composition for an explicitly supplied test-fleet application database binding."""

from __future__ import annotations

from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any

import psycopg
from juntai.observability import ObservabilityConfig, configure_observability

from juntai_synthetic_data.persistence import SqlGenerationRepository
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.providers import DeterministicTabularProvider, ProviderRegistry
from juntai_synthetic_data.service import SyntheticDataService


def build_runtime_service(
    *,
    connector: Callable[[], AbstractContextManager[Any]],
    test_fleet: bool,
    service_image_digest: str | None = None,
    otlp_endpoint: str = "http://127.0.0.1:4318",
) -> SyntheticDataService:
    if not test_fleet:
        raise RuntimeError("Synthetic Data Generation may run only in a test fleet")
    configure_observability(
        ObservabilityConfig(
            service_namespace="juntai",
            service_name="synthetic-data-generation",
            service_version="1.3.0",
            deployment_environment="test-fleet",
            artifact_digest=service_image_digest,
            otlp_endpoint=otlp_endpoint,
        )
    )
    provider = DeterministicTabularProvider()
    return SyntheticDataService(
        repository=SqlGenerationRepository(connector),
        providers=ProviderRegistry((provider,)),
        policy=DefaultPolicyEngine(),
    )


def psycopg_connector(dsn: str) -> Callable[[], AbstractContextManager[Any]]:
    return lambda: psycopg.connect(dsn)
