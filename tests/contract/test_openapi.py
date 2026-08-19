from __future__ import annotations

import json
from pathlib import Path

from conftest import make_service
from juntai.sdk.fuse_api import OpenAPIArtifactGenerator, ServiceArtifactIdentity

from juntai_synthetic_data.api import build_job_group
from juntai_synthetic_data.api.openapi import IAM_AUDIENCE, apply_bearer_security

ROOT = Path(__file__).parents[2]


def test_fuseapi_openapi_is_deterministic_and_complete() -> None:
    group = build_job_group(make_service())
    generator = OpenAPIArtifactGenerator(fuse_api_version="2.0.0")
    identity = ServiceArtifactIdentity(
        service="synthetic-data-generation", version="1.2.0", source_commit="a" * 40
    )
    first = generator.generate([group], identity=identity, title="Juntai Synthetic Data Generation")
    second = generator.generate(
        [group], identity=identity, title="Juntai Synthetic Data Generation"
    )
    assert first.digest == second.digest
    document = apply_bearer_security(json.loads(first.files[first.openapi_path]))
    assert set(document["paths"]) == {
        "/v1/jobs/",
        "/v1/jobs/{job_id}",
        "/v1/jobs/{job_id}:cancel",
        "/v1/jobs/{job_id}/result",
    }
    serialized = json.dumps(document).lower()
    assert "kes" not in serialized
    assert "oci credential" not in serialized
    assert "target_namespace" not in serialized
    assert document["components"]["securitySchemes"]["bearerAuth"] == {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
        "description": f"Casdoor access token with exact audience {IAM_AUDIENCE}.",
    }
    for path in document["paths"].values():
        for operation in path.values():
            assert operation["security"] == [{"bearerAuth": []}]
            assert operation["x-juntai-iam"]["audience"] == IAM_AUDIENCE


def test_committed_openapi_matches_generated_contract() -> None:
    committed = json.loads((ROOT / "openapi" / "synthetic-data-generation.v1.json").read_text())
    assert committed["info"]["version"] == "1.2.0"
    assert "/v1/jobs/" in committed["paths"]
    assert all(
        operation["security"] == [{"bearerAuth": []}]
        for path in committed["paths"].values()
        for operation in path.values()
    )
