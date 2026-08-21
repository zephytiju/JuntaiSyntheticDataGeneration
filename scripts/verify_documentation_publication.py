"""Verify the exact 01R pin, static catalog handoff, and no-reinterpretation boundary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from juntai.documentation.canonical import canonical_json_bytes
from juntai.documentation.catalog import select_capability, verify_catalog_signature
from juntai.documentation.loader import load_capability_set


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", required=True)
    parser.add_argument("--publication", required=True)
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--selection-request", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()

    build = Path(args.build)
    publication = json.loads(Path(args.publication).read_text(encoding="utf-8"))
    catalog = json.loads(Path(args.catalog).read_text(encoding="utf-8"))
    request = json.loads(Path(args.selection_request).read_text(encoding="utf-8"))
    pin = publication["pin"]
    loaded = load_capability_set(
        build,
        pin=pin,
        runtime_build_id=pin["producerBuildId"],
        runtime_openapi_digest=pin["openapiDigest"],
        runtime_mcp_descriptor_digest=pin["mcpDescriptorDigest"],
    )
    verify_catalog_signature(
        catalog,
        lambda payload, signature: (
            payload.decode("ascii") == signature["signedDigest"]
            and signature["algorithm"] == "github-attestations/sigstore"
            and signature["value"].startswith("https://github.com/")
        ),
    )
    selected = select_capability(catalog, request)
    if selected != {"status": "selected", "pin": pin}:
        raise SystemExit("static catalog selection changed the exact bundle pin")
    if loaded["tools"] or loaded["prompts"]:
        raise SystemExit("HTTP-only source release must not gain MCP Tools or Prompts")
    content_graph = json.loads((build / "content-graph.json").read_text(encoding="utf-8"))
    agent_unit_ids = {
        unit["unitId"] for unit in content_graph["units"] if "agent" in unit["audiences"]
    }
    resource_unit_ids = {
        unit_id for resource in loaded["resources"] for unit_id in resource["unitIds"]
    }
    if resource_unit_ids != agent_unit_ids:
        raise SystemExit("every reviewed agent unit must project to one MCP Resource")
    if not loaded["safetyPolicyUnitIds"]:
        raise SystemExit("publication lost required safety and policy units")

    result = {
        "schemaVersion": "synthetic-data.documentation/verification-result/v1",
        "status": "verified",
        "pin": pin,
        "catalogIndexDigest": catalog["indexDigest"],
        "catalogSignatureInputBound": True,
        "selectedWithoutReinterpretation": True,
        "unitCount": len(loaded["units"]),
        "resourceCount": len(loaded["resources"]),
        "promptCount": len(loaded["prompts"]),
        "toolCount": len(loaded["tools"]),
        "safetyPolicyUnitIds": loaded["safetyPolicyUnitIds"],
    }
    Path(args.out).write_bytes(canonical_json_bytes(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
