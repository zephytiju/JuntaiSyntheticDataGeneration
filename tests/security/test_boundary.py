from __future__ import annotations

import ast
import tomllib
from pathlib import Path

from juntai_synthetic_data.providers import DeterministicTabularProvider

ROOT = Path(__file__).parents[2]
SOURCE = ROOT / "src" / "juntai_synthetic_data"


def test_no_domain_platform_queue_or_artifact_imports() -> None:
    forbidden = {
        "lattice",
        "axiom",
        "juntai_platform_queue_kafka",
        "juntai_platform_swp_stream",
        "kafka",
    }
    imported: set[str] = set()
    for path in SOURCE.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0].lower() for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0].lower())
    assert not (imported & forbidden)
    dependencies = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["dependencies"]
    assert not any(
        "artifact" in item.lower() or "platform-" in item.lower() for item in dependencies
    )


def test_runtime_tree_has_no_async_execution_or_artifact_surface() -> None:
    paths = {path.relative_to(SOURCE).as_posix() for path in SOURCE.rglob("*.py")}
    forbidden_parts = {
        "worker.py",
        "worker_runtime.py",
        "relay_runtime.py",
        "worker_stream_runtime.py",
    }
    assert not (paths & forbidden_parts)
    for directory in (
        "worker_protocol",
        "execution",
        "publication",
        "validators",
        "jobs",
        "relay",
    ):
        assert not any(path.startswith(f"{directory}/") for path in paths)


def test_forward_migration_removes_legacy_job_and_worker_tables() -> None:
    migration = (ROOT / "migrations" / "0003_synchronous_generations.sql").read_text().lower()
    assert "create table juntai_synthetic_data.generations" in migration
    assert "create table juntai_synthetic_data.generation_rows" in migration
    assert migration.count("force row level security") == 2
    for table in (
        "worker_cleanup_evidence",
        "worker_result_inbox",
        "worker_outbox",
        "job_attempts",
        "job_transitions",
        "jobs",
    ):
        assert f"drop table if exists juntai_synthetic_data.{table}" in migration
    assert "enable row level security" in migration


def test_provider_is_an_in_process_library_without_worker_contract() -> None:
    manifest = DeterministicTabularProvider().manifest
    assert manifest.provider_id == "juntai.synthetic-data.tabular"
    assert not hasattr(manifest, "worker_image_digest")
    assert not hasattr(manifest, "network_policy")


def test_repository_contains_no_service_local_iac() -> None:
    suffixes = {
        path.suffix
        for path in ROOT.rglob("*")
        if path.is_file() and not {".venv", ".git", "build", "dist"} & set(path.parts)
    }
    assert ".tf" not in suffixes
    assert ".ts" not in suffixes


def test_image_is_non_root_and_has_no_external_build_context() -> None:
    lines = (ROOT / "Dockerfile").read_text().splitlines()
    runtime_bases = [line for line in lines if line.startswith("FROM python:3.13-slim@sha256:")]
    assert len(runtime_bases) == 1
    assert len(runtime_bases[0].rsplit("sha256:", 1)[1]) == 64
    assert "USER 65532:65532" in lines
    assert not any("adapter-artifacts" in line or "iam-artifacts" in line for line in lines)


def test_request_models_contain_no_connection_or_tenant_selector() -> None:
    models = (SOURCE / "contracts" / "models.py").read_text().lower()
    for forbidden in ("dsn:", "host:", "database:", "tenant_id:", "raw_sql"):
        assert forbidden not in models
