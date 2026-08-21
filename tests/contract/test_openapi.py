from __future__ import annotations

import json
from pathlib import Path

from conftest import make_service
from juntai.sdk.fuse_api import OpenAPIArtifactGenerator, ServiceArtifactIdentity

from juntai_synthetic_data.api import build_generation_group
from juntai_synthetic_data.api.openapi import IAM_AUDIENCE, apply_bearer_security

ROOT = Path(__file__).parents[2]


def test_fuseapi_openapi_is_deterministic_and_complete() -> None:
    group = build_generation_group(make_service())
    generator = OpenAPIArtifactGenerator(fuse_api_version="2.0.0")
    identity = ServiceArtifactIdentity(
        service="synthetic-data-generation", version="1.3.0", source_commit="a" * 40
    )
    first = generator.generate([group], identity=identity, title="Juntai Synthetic Data Generation")
    second = generator.generate(
        [group], identity=identity, title="Juntai Synthetic Data Generation"
    )
    assert first.digest == second.digest
    document = apply_bearer_security(json.loads(first.files[first.openapi_path]))
    assert set(document["paths"]) == {
        "/v1/generations",
        "/v1/generations/{generation_id}",
    }
    operations = {
        operation["operationId"]
        for path in document["paths"].values()
        for operation in path.values()
    }
    assert operations == {
        "syntheticData.createGeneration",
        "syntheticData.getGeneration",
        "syntheticData.deleteGeneration",
    }
    serialized = json.dumps(document).lower()
    for forbidden in (
        "/v1/jobs",
        "artifactreference",
        "target_namespace",
        "database credential",
        "kafka",
        "worker",
    ):
        assert forbidden not in serialized
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
            assert operation["x-juntai-iam"]["tenantSource"].startswith("verified-")


def test_checked_in_openapi_matches_generator() -> None:
    generated = json.loads((ROOT / "openapi" / "synthetic-data-generation.v1.json").read_text())
    assert set(generated["paths"]) == {
        "/v1/generations",
        "/v1/generations/{generation_id}",
    }
