"""Generate checksums, SPDX SBOM, and in-toto provenance for 01R release assets."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from datetime import UTC, datetime
from pathlib import Path

DOCUMENTED_SOURCE_COMMIT = "0000000000000000000000000000000000000000"


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value, ensure_ascii=False, allow_nan=False, sort_keys=True, separators=(",", ":")
        )
        + "\n"
    ).encode()


def _digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _created_at() -> str:
    epoch = int(os.environ.get("SOURCE_DATE_EPOCH", "0"))
    return datetime.fromtimestamp(epoch, tz=UTC).isoformat().replace("+00:00", "Z")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--assets", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--release-commit", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    output = Path(args.assets)
    artifacts = [
        path
        for path in sorted(output.iterdir())
        if path.is_file()
        and path.name
        not in {"SHA256SUMS", "release.spdx.json", "release.intoto.jsonl", "release-manifest.json"}
    ]
    subjects = [
        {"name": path.name, "digest": {"sha256": _digest(path)}, "byteLength": path.stat().st_size}
        for path in artifacts
    ]
    (output / "SHA256SUMS").write_text(
        "".join(f"{item['digest']['sha256']}  {item['name']}\n" for item in subjects),
        encoding="utf-8",
        newline="\n",
    )
    sbom = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "juntai-synthetic-data-generation-documentation-1.3.0",
        "documentNamespace": f"https://github.com/{args.repository}/actions/runs/{args.run_id}/sbom",
        "creationInfo": {
            "created": _created_at(),
            "creators": ["Tool: JuntaiDocumentationCapabilityBundle-1.0.0"],
        },
        "packages": [
            {
                "name": item["name"],
                "SPDXID": f"SPDXRef-Package-{index}",
                "versionInfo": "1.3.0",
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "checksums": [{"algorithm": "SHA256", "checksumValue": item["digest"]["sha256"]}],
                "licenseConcluded": "NOASSERTION",
                "licenseDeclared": "Apache-2.0",
                "copyrightText": "Copyright 2026 Juntai Team",
            }
            for index, item in enumerate(subjects, start=1)
        ],
    }
    (output / "release.spdx.json").write_bytes(_canonical(sbom))
    provenance = {
        "_type": "https://in-toto.io/Statement/v1",
        "subject": [{"name": item["name"], "digest": item["digest"]} for item in subjects],
        "predicateType": "https://slsa.dev/provenance/v1",
        "predicate": {
            "buildDefinition": {
                "buildType": (
                    f"https://github.com/{args.repository}/.github/workflows/"
                    "release-documentation.yml@refs/heads/main"
                ),
                "externalParameters": {
                    "bundleVersion": "1.3.0",
                    "documentedSourceCommit": DOCUMENTED_SOURCE_COMMIT,
                },
                "internalParameters": {},
                "resolvedDependencies": [
                    {
                        "uri": f"git+https://github.com/{args.repository}@{args.release_commit}",
                        "digest": {"gitCommit": args.release_commit},
                    },
                    {
                        "uri": f"git+https://github.com/{args.repository}@{DOCUMENTED_SOURCE_COMMIT}",
                        "digest": {"gitCommit": DOCUMENTED_SOURCE_COMMIT},
                    },
                ],
            },
            "runDetails": {
                "builder": {"id": "https://github.com/actions/runner"},
                "metadata": {
                    "invocationId": f"https://github.com/{args.repository}/actions/runs/{args.run_id}",
                    "startedOn": _created_at(),
                },
            },
        },
    }
    (output / "release.intoto.jsonl").write_bytes(_canonical(provenance))
    publication = json.loads((output / "CapabilityPublicationResult.json").read_text())
    manifest = {
        "schemaVersion": "synthetic-data.documentation/release-manifest/v1",
        "repository": args.repository,
        "releaseCommit": args.release_commit,
        "documentedSourceCommit": DOCUMENTED_SOURCE_COMMIT,
        "version": "1.3.0",
        "documentationCapabilityVersion": "1.0.0",
        "documentationCapabilitySource": "d7cd9b6860b7a716b210efdf66ae20e0f1d87a33",
        "documentationCapabilityWheelDigest": (
            "sha256:82995a96601f8249ca85bfd51cfb5fe34c3a2d8608ff7b0d42c5004a59843c33"
        ),
        "openapiDigest": publication["pin"]["openapiDigest"],
        "mcpDescriptorDigest": publication["pin"]["mcpDescriptorDigest"],
        "artifacts": subjects,
    }
    (output / "release-manifest.json").write_bytes(_canonical(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
