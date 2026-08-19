"""Freeze the exact Synthetic OpenAPI and FuseAPI 2.0.0 MCP descriptor for 01R."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

from juntai.sdk.fuse_api import MCPArtifactGenerator, MCPArtifactIdentity

from juntai_synthetic_data.api import build_job_group
from juntai_synthetic_data.service import SyntheticDataService

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTED_SOURCE_COMMIT = "2a4bd9ec4d33c8a7ef2d0f5ca1ee9155208ffa5b"
OPENAPI_DIGEST = "sha256:a1b68d7f8a76807b55e8707c49b88679e9a2ef288bc5d8d9966dd1fd4cafab60"
MCP_DESCRIPTOR_DIGEST = "sha256:5304fcfce8234a2428f83f8785dd2d8d6b34f32ff67db3df5464829084301e9a"


def _digest(payload: bytes) -> str:
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"


def main() -> int:
    destination = ROOT / "documentation" / "contracts"
    destination.mkdir(parents=True, exist_ok=True)
    openapi = (ROOT / "openapi" / "synthetic-data-generation.v1.json").read_bytes()
    if _digest(openapi) != OPENAPI_DIGEST:
        raise SystemExit("committed Synthetic OpenAPI differs from the documented source release")
    (destination / "openapi.json").write_bytes(openapi)
    (destination / "openapi.sha256").write_text(
        f"{OPENAPI_DIGEST.removeprefix('sha256:')}  openapi.json\n",
        encoding="utf-8",
        newline="\n",
    )

    identity = MCPArtifactIdentity(
        service="synthetic-data-generation",
        version="1.0.0",
        build_id=DOCUMENTED_SOURCE_COMMIT,
        source_commit=DOCUMENTED_SOURCE_COMMIT,
        openapi_sha256=OPENAPI_DIGEST,
    )
    group = build_job_group(cast(SyntheticDataService, object()))
    generated = MCPArtifactGenerator(fuse_api_version="2.0.0").generate([group], identity=identity)
    descriptor = generated.files[generated.descriptor_path].encode("utf-8")
    document = json.loads(descriptor)
    if document["tools"] != []:
        raise SystemExit("documented HTTP-only release unexpectedly generated MCP Tools")
    if _digest(descriptor) != MCP_DESCRIPTOR_DIGEST:
        raise SystemExit("Synthetic MCP descriptor differs from the reviewed exact digest")
    (destination / "mcp-descriptor.json").write_bytes(descriptor)
    (destination / "mcp-descriptor.sha256").write_text(
        f"{MCP_DESCRIPTOR_DIGEST.removeprefix('sha256:')}  mcp-descriptor.json\n",
        encoding="utf-8",
        newline="\n",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
