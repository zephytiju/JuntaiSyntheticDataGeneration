from __future__ import annotations

import ast
from pathlib import Path

from conftest import IMAGE_DIGEST

from juntai_synthetic_data.providers import DeterministicTabularProvider

ROOT = Path(__file__).parents[2]
SOURCE = ROOT / "src" / "juntai_synthetic_data"


def test_no_domain_or_target_store_imports() -> None:
    forbidden = {"lattice", "axiom", "kingbase", "kes"}
    imported: set[str] = set()
    for path in SOURCE.rglob("*.py"):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                imported.update(alias.name.split(".")[0].lower() for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module.split(".")[0].lower())
    assert not (imported & forbidden)


def test_runtime_has_no_target_store_or_preview_configuration() -> None:
    runtime = (SOURCE / "runtime.py").read_text().lower()
    for forbidden in ("target_namespace", "preview", "promotion", "kes_credential", "kes_dsn"):
        assert forbidden not in runtime


def test_migration_contains_only_bounded_job_metadata() -> None:
    migration = (ROOT / "migrations" / "0001_jobs.sql").read_text().lower()
    assert "create table if not exists juntai_synthetic_data.jobs" in migration
    assert "create table if not exists juntai_synthetic_data.job_transitions" in migration
    assert "dataset_rows" not in migration
    assert "payload_bytes" not in migration
    assert "octet_length" in migration


def test_provider_requires_digest_pinned_no_network_worker() -> None:
    manifest = DeterministicTabularProvider(worker_image_digest=IMAGE_DIGEST).manifest
    assert manifest.worker_image_digest.startswith("sha256:")
    assert manifest.network_policy == "deny-all"


def test_repository_contains_no_service_local_iac() -> None:
    suffixes = {path.suffix for path in ROOT.rglob("*") if path.is_file()}
    assert ".tf" not in suffixes
    assert ".ts" not in suffixes


def test_worker_base_image_is_digest_pinned() -> None:
    lines = (ROOT / "Dockerfile").read_text().splitlines()
    runtime_bases = [line for line in lines if line.startswith("FROM python:3.13-slim@sha256:")]
    assert len(runtime_bases) == 1
    assert len(runtime_bases[0].rsplit("sha256:", 1)[1]) == 64
    assert "FROM adapter-artifacts AS adapter-artifacts" in lines
    assert "FROM iam-artifacts AS iam-artifacts" in lines
