from __future__ import annotations

from unittest.mock import AsyncMock, Mock, patch

import pytest

from juntai_synthetic_data.api.openapi import IAM_AUDIENCE
from juntai_synthetic_data.cli import TEST_FLEET_ENV, _run_server


@pytest.mark.parametrize("value", [None, "", "TRUE", "True", "1", " true", "true "])
def test_serve_rejects_every_non_exact_test_fleet_value_before_construction(
    value: str | None,
) -> None:
    environment = {} if value is None else {TEST_FLEET_ENV: value}
    with (
        patch.dict("os.environ", environment, clear=True),
        patch("juntai_synthetic_data.migration.read_dsn_file") as read_dsn,
        patch("juntai_synthetic_data.runtime.psycopg_connector") as connector,
        patch("juntai_synthetic_data.runtime.build_runtime_service") as build_service,
        pytest.raises(RuntimeError, match=f"{TEST_FLEET_ENV} must be exactly lowercase true"),
    ):
        _run_server()

    read_dsn.assert_not_called()
    connector.assert_not_called()
    build_service.assert_not_called()


def test_exact_test_fleet_value_constructs_and_serves_the_runtime() -> None:
    server = Mock()
    server.serve = AsyncMock()
    environment = {
        TEST_FLEET_ENV: "true",
        "JUNTAI_IAM_ISSUER": "https://iam.test.example",
        "JUNTAI_IAM_AUDIENCE": IAM_AUDIENCE,
        "JUNTAI_IAM_POLICY_SNAPSHOT": "/run/secrets/juntai/iam-policy.json",
        "JUNTAI_IAM_DISCOVERY_URL": "https://iam.test.example/.well-known/openid-configuration",
        "JUNTAI_SERVICE_IMAGE_DIGEST": "sha256:" + "1" * 64,
        "OTEL_EXPORTER_OTLP_ENDPOINT": "http://otel.test:4318",
        "HOST": "127.0.0.1",
    }
    connector = Mock()
    service = Mock()
    authorizer = Mock()
    with (
        patch.dict("os.environ", environment, clear=True),
        patch("juntai_synthetic_data.migration.read_dsn_file", return_value="dsn") as read_dsn,
        patch(
            "juntai_synthetic_data.runtime.psycopg_connector", return_value=connector
        ) as build_connector,
        patch(
            "juntai_synthetic_data.runtime.build_runtime_service", return_value=service
        ) as build_service,
        patch(
            "juntai_synthetic_data.runtime_auth.build_runtime_authorizer",
            return_value=authorizer,
        ) as build_authorizer,
        patch("juntai_synthetic_data.api.build_server", return_value=server) as build_server,
    ):
        _run_server()

    read_dsn.assert_called_once_with()
    build_connector.assert_called_once_with("dsn")
    build_authorizer.assert_called_once_with(
        issuer="https://iam.test.example",
        audiences=(IAM_AUDIENCE,),
        policy_snapshot_path="/run/secrets/juntai/iam-policy.json",
        discovery_url="https://iam.test.example/.well-known/openid-configuration",
    )
    build_service.assert_called_once_with(
        connector=connector,
        test_fleet=True,
        service_image_digest="sha256:" + "1" * 64,
        otlp_endpoint="http://otel.test:4318",
    )
    build_server.assert_called_once_with(service, authorizer=authorizer)
    server.serve.assert_awaited_once_with(host="127.0.0.1")


def test_generic_environment_does_not_satisfy_admission() -> None:
    with (
        patch.dict("os.environ", {"JUNTAI_ENVIRONMENT": "development"}, clear=True),
        pytest.raises(RuntimeError, match=f"{TEST_FLEET_ENV} must be exactly lowercase true"),
    ):
        _run_server()
