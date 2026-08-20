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
    assert manifest["metadata"]["producerBuildId"] == ("5e0a11759457b76f4ed8e232128921a78c710806")
    assert manifest["provenance"]["sourceCommit"] == manifest["metadata"]["producerBuildId"]
    openapi = manifest["contracts"]["openapi"]
    descriptor = manifest["contracts"]["mcp"]
    assert openapi["digest"] == _digest(DOCUMENTATION / openapi["path"])
    assert descriptor["digest"] == descriptor["descriptorDigest"]
    assert descriptor["digest"] == _digest(DOCUMENTATION / descriptor["path"])
    assert openapi["digest"] == (
        "sha256:ae36c89103a4cf341111c2001ab35ae76f06335d9d5168bdd29d637c4837ee2b"
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
        "syntheticData.cancelJob",
        "syntheticData.createJob",
        "syntheticData.getJob",
        "syntheticData.getJobResult",
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


def test_documentation_contains_no_product_domain_semantics() -> None:
    sources = [
        path
        for path in DOCUMENTATION.rglob("*")
        if path.is_file() and path.suffix in {".md", ".json"} and "contracts" not in path.parts
    ]
    authored = "\n".join(path.read_text().lower() for path in sources)
    for excluded in ("lattice", "axiom", "prism", "vangu"):
        assert excluded not in authored
