from __future__ import annotations

import ast
import tomllib
from pathlib import Path

import pytest

from juntai_synthetic_data.worker import validate_worker_isolation
from juntai_synthetic_data.worker_stream_runtime import STREAM_ENVIRONMENT

ROOT = Path(__file__).parents[2]
SOURCE = ROOT / "src" / "juntai_synthetic_data"


def allowed_environment() -> dict[str, str]:
    return {name: "test-only" for name in STREAM_ENVIRONMENT}


def test_worker_accepts_only_the_exact_stream_projection_without_forbidden_mounts() -> None:
    validate_worker_isolation(allowed_environment(), mountinfo="tmpfs /var/run/juntai-worker-tmp")


@pytest.mark.parametrize(
    "name",
    [
        "JUNTAI_JOB_DATABASE_DSN",
        "JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE",
        "JUNTAI_QUEUE_ENDPOINT",
        "JUNTAI_QUEUE_TRANSPORT_FACTORY",
        "JUNTAI_QUEUE_CREDENTIAL_FILE",
        "JUNTAI_QUEUE_DISPATCH_ENDPOINT",
        "JUNTAI_QUEUE_CONTROL_ENDPOINT",
        "JUNTAI_QUEUE_RESULT_ENDPOINT",
        "JUNTAI_QUEUE_DEAD_LETTER_ENDPOINT",
        "JUNTAI_SYNTHETIC_API_BASE_URL",
        "KUBERNETES_SERVICE_HOST",
        "KUBERNETES_TOKEN_REVIEW_TOKEN_FILE",
        "JUNTAI_SYNTHETIC_WORKER_PROTOCOL",
        "JUNTAI_SYNTHETIC_WORKER_SOCKET",
        "JUNTAI_SWP_CLIENT_PRIVATE_KEY_FILE",
    ],
)
def test_worker_rejects_every_kes_queue_api_or_kubernetes_setting(name: str) -> None:
    environment = allowed_environment()
    environment[name] = "forbidden"
    with pytest.raises(RuntimeError, match="forbidden worker configuration"):
        validate_worker_isolation(environment, mountinfo="")


@pytest.mark.parametrize(
    "mount",
    [
        "kes-dsn",
        "kingbase/client.pem",
        "queue-token",
        "token-reviewer/token",
        "serviceaccount/token",
        "kubeconfig",
        "juntai-worker/swp-v1.sock",
    ],
)
def test_worker_rejects_forbidden_credentials_and_mounts(mount: str) -> None:
    with pytest.raises(RuntimeError, match="credential mount"):
        validate_worker_isolation(allowed_environment(), mountinfo=f"tmpfs /var/run/{mount}")


def test_worker_import_closure_has_no_sql_repository_or_database_driver() -> None:
    files = [
        SOURCE / "worker.py",
        SOURCE / "worker_runtime.py",
        SOURCE / "worker_stream_runtime.py",
        SOURCE / "execution" / "worker_engine.py",
        SOURCE / "execution" / "artifacts.py",
        SOURCE / "worker_protocol" / "models.py",
        SOURCE / "worker_protocol" / "framing.py",
    ]
    imported: set[str] = set()
    combined = ""
    for path in files:
        combined += path.read_text()
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
    assert all(
        not name.startswith(
            (
                "psycopg",
                "sqlalchemy",
                "confluent_kafka",
                "kafka",
                "juntai_platform_queue_kafka",
                "juntai_synthetic_data.relay",
            )
        )
        for name in imported
    )
    assert "SqlJobRepository" not in combined
    assert "QueueTransport" not in combined
    assert "relay_runtime" not in combined
    assert "JUNTAI_JOB_DATABASE_DSN" not in (SOURCE / "worker_runtime.py").read_text()


def test_runtime_dependencies_do_not_include_broker_or_kubernetes_clients() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]
    dependencies = [dependency.lower() for dependency in project["dependencies"]]
    forbidden = (
        "confluent-kafka",
        "kafka-python",
        "aiokafka",
        "pika",
        "kombu",
        "celery",
        "kubernetes",
    )
    assert all(not dependency.startswith(forbidden) for dependency in dependencies)
