"""Freeze the exact Synthetic 1.3.0 OpenAPI and FuseAPI MCP descriptor."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

import yaml
from juntai.sdk.fuse_api import MCPArtifactGenerator, MCPArtifactIdentity

from juntai_synthetic_data.api import build_generation_group
from juntai_synthetic_data.service import SyntheticDataService

ROOT = Path(__file__).resolve().parents[1]


def _digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def main() -> int:
    destination = ROOT / "documentation" / "contracts"
    destination.mkdir(parents=True, exist_ok=True)
    manifest = yaml.safe_load((ROOT / "documentation" / "manifest.yaml").read_text())
    documented_source_commit = manifest["metadata"]["producerBuildId"]
    openapi_digest = manifest["contracts"]["openapi"]["digest"]
    openapi = (ROOT / "openapi" / "synthetic-data-generation.v1.json").read_bytes()
    if _digest(openapi) != openapi_digest:
        raise SystemExit("committed Synthetic OpenAPI differs from the documented source release")
    (destination / "openapi.json").write_bytes(openapi)
    (destination / "openapi.sha256").write_text(
        f"{openapi_digest.removeprefix('sha256:')}  openapi.json\n",
        encoding="utf-8",
        newline="\n",
    )

    identity = MCPArtifactIdentity(
        service="synthetic-data-generation",
        version="1.3.0",
        build_id=documented_source_commit,
        source_commit=documented_source_commit,
        openapi_sha256=openapi_digest,
    )
    group = build_generation_group(cast(SyntheticDataService, object()))
    generated = MCPArtifactGenerator(fuse_api_version="2.0.0").generate([group], identity=identity)
    descriptor = generated.files[generated.descriptor_path].encode("utf-8")
    document = json.loads(descriptor)
    if document["tools"] != []:
        raise SystemExit("documented HTTP-only release unexpectedly generated MCP Tools")
    descriptor_digest = _digest(descriptor)
    (destination / "mcp-descriptor.json").write_bytes(descriptor)
    (destination / "mcp-descriptor.sha256").write_text(
        f"{descriptor_digest.removeprefix('sha256:')}  mcp-descriptor.json\n",
        encoding="utf-8",
        newline="\n",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
