from __future__ import annotations

import ast
from pathlib import Path

import pytest

from juntai_synthetic_data.worker import (
    PROTOCOL_ENV,
    SOCKET_ENV,
    validate_worker_isolation,
)
from juntai_synthetic_data.worker_protocol import PROTOCOL_VERSION, SOCKET_PATH

ROOT = Path(__file__).parents[2]
SOURCE = ROOT / "src" / "juntai_synthetic_data"


def allowed_environment() -> dict[str, str]:
    return {PROTOCOL_ENV: PROTOCOL_VERSION, SOCKET_ENV: SOCKET_PATH}


def test_worker_accepts_only_the_exact_socket_protocol_without_forbidden_mounts() -> None:
    validate_worker_isolation(allowed_environment(), mountinfo="tmpfs /var/run/juntai-worker")


@pytest.mark.parametrize(
    "name",
    [
        "JUNTAI_JOB_DATABASE_DSN",
        "JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE",
        "JUNTAI_QUEUE_ENDPOINT",
        "JUNTAI_SYNTHETIC_API_BASE_URL",
        "KUBERNETES_SERVICE_HOST",
        "KUBERNETES_TOKEN_REVIEW_TOKEN_FILE",
    ],
)
def test_worker_rejects_every_kes_queue_api_or_kubernetes_setting(name: str) -> None:
    environment = allowed_environment()
    environment[name] = "forbidden"
    with pytest.raises(RuntimeError, match="forbidden worker configuration"):
        validate_worker_isolation(environment, mountinfo="")


@pytest.mark.parametrize("mount", ["kes-dsn", "queue-token", "serviceaccount/token"])
def test_worker_rejects_forbidden_credentials_and_mounts(mount: str) -> None:
    with pytest.raises(RuntimeError, match="credential mount"):
        validate_worker_isolation(allowed_environment(), mountinfo=f"tmpfs /var/run/{mount}")


def test_worker_import_closure_has_no_sql_repository_or_database_driver() -> None:
    files = [
        SOURCE / "worker.py",
        SOURCE / "worker_runtime.py",
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
    assert all(not name.startswith(("psycopg", "sqlalchemy")) for name in imported)
    assert "SqlJobRepository" not in combined
    assert "JUNTAI_JOB_DATABASE_DSN" not in (SOURCE / "worker_runtime.py").read_text()
