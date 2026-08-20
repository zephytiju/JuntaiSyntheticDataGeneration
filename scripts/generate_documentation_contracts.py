"""Freeze the exact Synthetic 1.3.0 OpenAPI and FuseAPI MCP descriptor."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import cast

from juntai.sdk.fuse_api import MCPArtifactGenerator, MCPArtifactIdentity

from juntai_synthetic_data.api import build_job_group
from juntai_synthetic_data.service import SyntheticDataService

ROOT = Path(__file__).resolve().parents[1]
DOCUMENTED_SOURCE_COMMIT = "5e0a11759457b76f4ed8e232128921a78c710806"
OPENAPI_DIGEST = "sha256:ae36c89103a4cf341111c2001ab35ae76f06335d9d5168bdd29d637c4837ee2b"


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
        version="1.3.0",
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
