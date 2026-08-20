"""Compose 01O build outputs into exact immutable release publication contracts."""

from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any

from juntai.documentation.canonical import canonical_json_bytes

REPOSITORY = "zephytiju/JuntaiSyntheticDataGeneration"
RELEASE_VERSION = "1.3.0"
PACKAGER_VERSION = "1.0.0"
DOCUMENTED_SOURCE_COMMIT = "7e42eea5ead03e889e8465f29339222d879bd536"
DOCUMENTATION_CAPABILITY_SOURCE = "d7cd9b6860b7a716b210efdf66ae20e0f1d87a33"
DOCUMENTATION_CAPABILITY_WHEEL = (
    "sha256:82995a96601f8249ca85bfd51cfb5fe34c3a2d8608ff7b0d42c5004a59843c33"
)


def _write(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(canonical_json_bytes(value))


def _coordinate(
    build: dict[str, Any], *, release_tag: str, asset: str, name: str, kind: str
) -> dict[str, Any]:
    digest = build["artifacts"][asset]["digest"]
    asset_url = f"https://github.com/{REPOSITORY}/releases/download/{release_tag}/{asset}"
    return {
        "ownerKey": build["ownerKey"],
        "bundleId": build["bundleId"],
        "version": build["version"],
        "artifactRef": {
            "artifact_id": asset_url,
            "version_id": release_tag,
            "manifest_digest": digest,
            "version": build["version"],
            "name": name,
            "kind": kind,
        },
        "digest": digest,
        "schemaMajor": 1,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--build", required=True)
    parser.add_argument("--out", required=True)
    parser.add_argument("--release-tag", required=True)
    parser.add_argument("--attestation-id", required=True)
    args = parser.parse_args()

    build_root = Path(args.build)
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    build = json.loads((build_root / "build-result.json").read_text(encoding="utf-8"))
    if build["packageVersion"] != PACKAGER_VERSION or build["version"] != RELEASE_VERSION:
        raise SystemExit("documentation build or packager version is not the reviewed tuple")
    if build["producerBuildId"] != DOCUMENTED_SOURCE_COMMIT:
        raise SystemExit("publication producerBuildId differs from the exact Synthetic source")

    definitions = {
        "bundle": (
            "capability-bundle.tar",
            "synthetic-data-generation-docs",
            "documentation.capability",
        ),
        "humanProjection": (
            "documentation-human.tar",
            "synthetic-data-generation-docs-human",
            "documentation.human",
        ),
        "mcpProjection": (
            "documentation-mcp.json",
            "synthetic-data-generation-docs-mcp",
            "documentation.mcp",
        ),
        "provenance": (
            "provenance.json",
            "synthetic-data-generation-docs-provenance",
            "capability.provenance",
        ),
    }
    coordinates = {
        part: _coordinate(build, release_tag=args.release_tag, asset=asset, name=name, kind=kind)
        for part, (asset, name, kind) in definitions.items()
    }
    pin = {
        "coordinate": coordinates["bundle"],
        "producerBuildId": build["producerBuildId"],
        "openapiDigest": build["openapiDigest"],
        "mcpDescriptorDigest": build["mcpDescriptorDigest"],
        "contentGraphDigest": build["contentGraphDigest"],
        "humanProjectionDigest": build["humanProjectionDigest"],
        "mcpProjectionDigest": build["mcpProjectionDigest"],
    }
    publication = {
        "schemaVersion": "capability.juntai.io/publication-result/v1",
        **coordinates,
        "pin": pin,
    }
    _write(output / "CapabilityPublicationResult.json", publication)
    _write(output / "CapabilityBundlePin.json", pin)
    _write(
        output / "source-binding.json",
        {
            "schemaVersion": "synthetic-data.documentation/source-binding/v1",
            "repository": f"https://github.com/{REPOSITORY}",
            "documentedSourceCommit": DOCUMENTED_SOURCE_COMMIT,
            "openapiDigest": build["openapiDigest"],
            "mcpDescriptorDigest": build["mcpDescriptorDigest"],
            "documentationCapabilitySource": DOCUMENTATION_CAPABILITY_SOURCE,
            "documentationCapabilityWheelDigest": DOCUMENTATION_CAPABILITY_WHEEL,
            "producerBuildId": build["producerBuildId"],
        },
    )
    record = {
        "title": "Synthetic Data Generation",
        "summary": (
            "Exact bearer-IAM, SWP/v1 worker isolation, bounded job, Artifact, provenance, "
            "migration, and safety documentation."
        ),
        "ownerKey": build["ownerKey"],
        "visibility": "internal",
        "lifecycle": "release",
        "locales": ["en"],
        "taskClasses": [
            "synthetic-data.jobs.cancel",
            "synthetic-data.jobs.create",
            "synthetic-data.jobs.monitor",
            "synthetic-data.jobs.result",
        ],
        "compatibility": {"platform": ">=4.0 <5.0"},
        "routeKey": "platform.documentation",
        "pin": pin,
    }
    _write(output / "catalog-record.json", record)
    _write(
        output / "catalog-signature.json",
        {
            "algorithm": "github-attestations/sigstore",
            "keyId": (
                f"https://github.com/{REPOSITORY}/.github/workflows/"
                f"release-documentation.yml@refs/tags/{args.release_tag}"
            ),
            "value": args.attestation_id,
        },
    )
    _write(
        output / "selection-request.json",
        {
            "ownerKey": build["ownerKey"],
            "bundleId": build["bundleId"],
            "version": build["version"],
            "digest": pin["coordinate"]["digest"],
            "producerBuildId": pin["producerBuildId"],
            "mcpDescriptorDigest": pin["mcpDescriptorDigest"],
            "openapiDigest": pin["openapiDigest"],
            "platformVersion": "4.0.0",
            "taskClass": "synthetic-data.jobs.result",
            "locale": "en",
        },
    )

    release_files = {
        "agent-capability-bundle.tar",
        "build-result.json",
        "capability-bundle.tar",
        "content-graph.json",
        "documentation-human.tar",
        "documentation-mcp.json",
        "human-projection.json",
        "mcp-projection.json",
        "provenance.json",
    }
    for name in sorted(release_files):
        shutil.copyfile(build_root / name, output / name)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
