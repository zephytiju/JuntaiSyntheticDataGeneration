from __future__ import annotations

import hashlib
import json
from pathlib import Path

import yaml

ROOT = Path(__file__).parents[2]
DOCUMENTATION = ROOT / "documentation"


def _digest(path: Path) -> str:
    return f"sha256:{hashlib.sha256(path.read_bytes()).hexdigest()}"


def test_documentation_contracts_pin_exact_source_release() -> None:
    manifest = yaml.safe_load((DOCUMENTATION / "manifest.yaml").read_text())
    assert len(manifest["metadata"]["producerBuildId"]) == 40
    assert manifest["provenance"]["sourceCommit"] == manifest["metadata"]["producerBuildId"]
    openapi = manifest["contracts"]["openapi"]
    descriptor = manifest["contracts"]["mcp"]
    assert openapi["digest"] == _digest(DOCUMENTATION / openapi["path"])
    assert descriptor["digest"] == descriptor["descriptorDigest"]
    assert descriptor["digest"] == _digest(DOCUMENTATION / descriptor["path"])
    assert openapi["digest"] == (
        "sha256:1baa48eeb089d9e2244ca601f6c454a4ea5e7acfff8140f3f88a86e5110ac38a"
    )


def test_http_only_descriptor_and_exact_operations_are_preserved() -> None:
    descriptor = json.loads((DOCUMENTATION / "contracts/mcp-descriptor.json").read_text())
    openapi = json.loads((DOCUMENTATION / "contracts/openapi.json").read_text())
    assert descriptor["fuseApiVersion"] == "2.0.0"
    assert descriptor["profile"] == "juntai.fuse.profile.mcp/v1"
    assert descriptor["tools"] == []
    assert descriptor["serviceVersion"] == "1.3.0"
    assert openapi["components"]["securitySchemes"]["bearerAuth"]["scheme"] == "bearer"
    operations = {
        operation["operationId"]
        for path in openapi["paths"].values()
        for operation in path.values()
    }
    assert operations == {
        "syntheticData.createGeneration",
        "syntheticData.deleteGeneration",
        "syntheticData.getGeneration",
    }


def test_one_reviewed_graph_has_resources_without_invented_prompts() -> None:
    manifest = yaml.safe_load((DOCUMENTATION / "manifest.yaml").read_text())
    units = manifest["sources"]["units"]
    assert len(units) >= 10
    assert all(unit["mcp"]["resourceId"] == unit["unitId"] for unit in units)
    assert all("promptId" not in unit["mcp"] for unit in units)
    assert any(unit["kind"] == "safety" for unit in units)
    assert any(unit["kind"] == "workflow" for unit in units)
    assert any(unit["kind"] == "example" for unit in units)


def test_documentation_contains_no_withdrawn_runtime_semantics() -> None:
    sources = [
        path
        for path in DOCUMENTATION.rglob("*")
        if path.is_file() and path.suffix in {".md", ".json"} and "contracts" not in path.parts
    ]
    authored = "\n".join(path.read_text().lower() for path in sources)
    for excluded in (
        "juntai-platform-queue-kafka",
        "juntai-platform-swp-stream",
        "platform_worker_delivery_v1",
        "juntai.synthetic.worker/v1",
        "juntai_synthetic_data_destination_allowlist_file",
    ):
        assert excluded not in authored
    assert "single application" in authored
    assert "logical destination" in authored
    assert "juntai_synthetic_data_test_fleet=true" in authored
    assert "juntai_environment" in authored


def test_destination_authority_has_no_service_policy_or_catalog_preflight() -> None:
    package = ROOT / "src/juntai_synthetic_data"
    source = "\n".join(
        path.read_text().lower()
        for path in package.rglob("*.py")
        if "migrations" not in path.parts and path.name != "migration.py"
    )
    for excluded in (
        "destinationallowlist",
        "validate_destinations",
        "information_schema.columns",
        "has_table_privilege",
        "pg_constraint",
        "pg_index",
    ):
        assert excluded not in source
