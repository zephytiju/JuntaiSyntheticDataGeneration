"""Compose the exact immutable Synthetic 1.3.0 service and migration release assets."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import zipfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).parents[1]
VERSION = "1.3.0"
RELEASE_TAG = "synthetic-data-v1.3.0"
IAM_SOURCE_COMMIT = "72b481ed825c00d0bd96feca67790e90dc5ace9b"
IAM_WHEEL_SHA256 = "007362537726dbd69c75952b73c62b90e4f7ea92a48ab214ba0ad3ffcb533e6c"
IAM_CONTRACTS_SOURCE_COMMIT = "a37b6d6daaba75efd8c15c19b440a3081ba761c5"
IAM_CONTRACTS_WHEEL_SHA256 = "e1daa81386669cfbf74b119c73f822d80a2f5e7a64a187538c54dcff07643cf1"
IAM_CONTRACT_MANIFEST_SHA256 = "64dafb25c54d40320347c8661960d23ba524a2d3c102d112c08c95679d12db85"
KES_IMAGE = (
    "kingbase_v009r001c010b0004_single_x86:v1@"
    "sha256:0bce318e74adca7a3d619b55b336269017507fd679833b7ce5d8400289661724"
)
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")
EXPECTED_KES_CHECKS = [
    "concurrency-lock",
    "cross-schema-atomic-write",
    "database-destination-rejection",
    "database-restart",
    "delete-idempotence",
    "destination-conflict-rollback",
    "empty-database",
    "exact-key-delete",
    "idempotent-replay",
    "ledger-current",
    "lost-response-recovery",
    "no-platform-database-dependency",
    "quoted-caller-destination",
    "released-1.2.0-baseline-upgrade",
    "repeat-idempotence",
    "tenant-rls-isolation",
    "transactional-failure-recovery",
    "transactional-partial-failure",
]


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


def _write(path: Path, value: object) -> None:
    path.write_bytes(_canonical(value))


def _wheel_version(path: Path) -> str:
    with zipfile.ZipFile(path) as archive:
        metadata_names = [
            name for name in archive.namelist() if name.endswith(".dist-info/METADATA")
        ]
        if len(metadata_names) != 1:
            raise SystemExit("wheel must contain exactly one METADATA file")
        metadata = archive.read(metadata_names[0]).decode()
    for line in metadata.splitlines():
        if line.startswith("Version: "):
            return line.removeprefix("Version: ")
    raise SystemExit("wheel METADATA has no Version")


def _artifact(path: Path) -> dict[str, object]:
    return {"name": path.name, "sha256": _digest(path), "byteLength": path.stat().st_size}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", required=True)
    parser.add_argument("--wheel", required=True)
    parser.add_argument("--real-kes-evidence", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--image", required=True)
    parser.add_argument("--image-digest", required=True)
    parser.add_argument("--run-id", required=True)
    return parser


def _validate(args: argparse.Namespace, evidence: dict[str, Any]) -> None:
    if not _COMMIT.fullmatch(args.source_revision):
        raise SystemExit("source revision must be an exact Git commit")
    if not _DIGEST.fullmatch(args.image_digest):
        raise SystemExit("service image must use an immutable digest")
    if evidence.get("result") != "passed":
        raise SystemExit("real-KES acceptance result is not passed")
    if evidence.get("sourceRevision") != args.source_revision:
        raise SystemExit("real-KES evidence source differs from release source")
    if evidence.get("serviceImageDigest") != args.image_digest:
        raise SystemExit("real-KES evidence image differs from release image")
    if evidence.get("kingbaseImage") != KES_IMAGE:
        raise SystemExit("real-KES evidence used an unexpected KingbaseES image")
    if evidence.get("kingbaseVersion") != "KingbaseES V009R001C010":
        raise SystemExit("real-KES evidence used an unexpected KingbaseES version")
    if evidence.get("serviceVersion") != VERSION:
        raise SystemExit("real-KES evidence used an unexpected service version")
    if evidence.get("schemaVersion") != "juntai.synthetic-data.real-kes-acceptance-result/v1":
        raise SystemExit("real-KES evidence used an unexpected evidence schema")
    if evidence.get("checks") != EXPECTED_KES_CHECKS:
        raise SystemExit("real-KES evidence did not pass the exact reviewed check matrix")
    if evidence.get("migrationIds") != [
        "0001_jobs",
        "0002_worker_protocol",
        "0003_synchronous_generations",
    ]:
        raise SystemExit("real-KES evidence used an unexpected migration set")


def main() -> int:
    args = _parser().parse_args()
    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=False)
    wheel = Path(args.wheel)
    if _wheel_version(wheel) != VERSION:
        raise SystemExit(f"wheel version must be {VERSION}")
    evidence = json.loads(Path(args.real_kes_evidence).read_text())
    _validate(args, evidence)

    sources = {
        wheel: wheel.name,
        ROOT / "migrations" / "0001_jobs.sql": "0001_jobs.sql",
        ROOT / "migrations" / "0002_worker_protocol.sql": "0002_worker_protocol.sql",
        ROOT / "migrations" / "0003_synchronous_generations.sql": (
            "0003_synchronous_generations.sql"
        ),
        ROOT / "migrations" / "manifest.v1.json": "migration-set.v1.json",
        ROOT / "openapi" / "synthetic-data-generation.v1.json": (
            "synthetic-data-generation.openapi.v1.json"
        ),
        ROOT / "openapi" / "synthetic-data-generation.v1.sha256": (
            "synthetic-data-generation.openapi.v1.sha256"
        ),
        ROOT / "fixtures" / "generation-request.v1.json": "generation-request.v1.json",
        ROOT / "fixtures" / "generation-records.v1.json": "generation-records.v1.json",
        ROOT / "fixtures" / "SHA256SUMS": "fixture-SHA256SUMS",
        ROOT / "documentation" / "contracts" / "openapi.json": ("documentation.openapi.v1.json"),
        ROOT / "documentation" / "contracts" / "mcp-descriptor.json": (
            "documentation.mcp-descriptor.v1.json"
        ),
        ROOT / "MIGRATIONS.md": "MIGRATIONS.md",
        ROOT / "scripts" / "real_kes_acceptance.py": "real_kes_acceptance.py",
        ROOT / "scripts" / "run-real-kes-acceptance.sh": "run-real-kes-acceptance.sh",
        Path(args.real_kes_evidence): "real-kes-acceptance.json",
    }
    for source, name in sources.items():
        shutil.copyfile(source, output / name)

    migration_set = json.loads((output / "migration-set.v1.json").read_text())
    for migration in migration_set["migrations"]:
        sql_digest = _digest(output / migration["path"])
        if migration["sha256"] != sql_digest:
            raise SystemExit(f"released SQL does not match checksum: {migration['id']}")
    openapi_digest = _digest(output / "synthetic-data-generation.openapi.v1.json")
    expected_openapi = (
        (output / "synthetic-data-generation.openapi.v1.sha256").read_text().split()[0]
    )
    if openapi_digest != expected_openapi:
        raise SystemExit("released OpenAPI checksum differs from its checksum file")
    image_reference = f"{args.image}@{args.image_digest}"
    wheel_asset = _artifact(output / wheel.name)
    migration_release = {
        "schemaVersion": "juntai.synthetic-data.migration-release/v1",
        "service": "synthetic-data-generation",
        "version": VERSION,
        "releaseTag": RELEASE_TAG,
        "releaseBinding": {
            "repository": f"https://github.com/{args.repository}",
            "sourceRevision": args.source_revision,
            "image": image_reference,
            "wheel": wheel_asset,
            "openapiSha256": openapi_digest,
            "githubRun": f"https://github.com/{args.repository}/actions/runs/{args.run_id}",
        },
        "execution": {
            "argv": ["juntai-synthetic-data", "migrate"],
            "dsnFileEnvironment": "JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE",
            "sourceRevisionEnvironment": "JUNTAI_SOURCE_REVISION",
            "imageDigestEnvironment": "JUNTAI_SERVICE_IMAGE_DIGEST",
            "successExitCode": 0,
            "checkPendingExitCode": 5,
        },
        "compatibility": migration_set["compatibility"],
        "database": migration_set["database"],
        "migrations": migration_set["migrations"],
        "realKesAcceptance": {
            "asset": "real-kes-acceptance.json",
            "sha256": _digest(output / "real-kes-acceptance.json"),
            "result": "passed",
            "executionContext": "independent-licensed-kes",
            "githubHosted": False,
            "completedAt": evidence["completedAt"],
            "checks": evidence["checks"],
        },
    }
    _write(output / "migration-release-manifest.json", migration_release)

    base_assets = [_artifact(path) for path in sorted(output.iterdir()) if path.is_file()]
    release_manifest = {
        "schemaVersion": "juntai.synthetic-data.service-release/v1",
        "repository": args.repository,
        "releaseTag": RELEASE_TAG,
        "version": VERSION,
        "sourceRevision": args.source_revision,
        "image": image_reference,
        "imageSbom": "OCI BuildKit SBOM attached to the image index",
        "imageProvenance": "GitHub artifact attestation and OCI BuildKit provenance",
        "runtimeBinding": {
            "argv": ["juntai-synthetic-data", "serve"],
            "testFleetAdmission": {
                "environmentVariable": "JUNTAI_SYNTHETIC_DATA_TEST_FLEET",
                "requiredValue": "true",
                "comparison": "exact",
            },
            "databaseDsnFileEnvironment": "JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE",
            "iamAudience": "juntai.synthetic-data.api",
        },
        "documentationOpenapiSha256": _digest(output / "documentation.openapi.v1.json"),
        "documentationMcpSha256": _digest(output / "documentation.mcp-descriptor.v1.json"),
        "migrationManifest": {
            "name": "migration-release-manifest.json",
            "sha256": _digest(output / "migration-release-manifest.json"),
        },
        "externalContracts": {
            "iam": {
                "version": "1.1.0",
                "sourceCommit": IAM_SOURCE_COMMIT,
                "wheelSha256": IAM_WHEEL_SHA256,
                "contractsVersion": "1.1.1",
                "contractsSourceCommit": IAM_CONTRACTS_SOURCE_COMMIT,
                "contractsWheelSha256": IAM_CONTRACTS_WHEEL_SHA256,
                "contractManifestSha256": IAM_CONTRACT_MANIFEST_SHA256,
            }
        },
        "artifacts": base_assets,
    }
    _write(output / "service-release-manifest.json", release_manifest)

    sbom_subjects = [_artifact(path) for path in sorted(output.iterdir()) if path.is_file()]
    sbom = {
        "spdxVersion": "SPDX-2.3",
        "dataLicense": "CC0-1.0",
        "SPDXID": "SPDXRef-DOCUMENT",
        "name": "juntai-synthetic-data-generation-1.3.0-release",
        "documentNamespace": f"https://github.com/{args.repository}/actions/runs/{args.run_id}/sbom",
        "creationInfo": {
            "created": _created_at(),
            "creators": ["Tool: generate_service_release.py-1.3.0"],
        },
        "packages": [
            {
                "name": item["name"],
                "SPDXID": f"SPDXRef-Package-{index}",
                "versionInfo": VERSION,
                "downloadLocation": "NOASSERTION",
                "filesAnalyzed": False,
                "checksums": [{"algorithm": "SHA256", "checksumValue": item["sha256"]}],
                "licenseConcluded": "NOASSERTION",
                "licenseDeclared": "Apache-2.0",
                "copyrightText": "Copyright 2026 Juntai Team",
            }
            for index, item in enumerate(sbom_subjects, start=1)
        ],
    }
    _write(output / "release.spdx.json", sbom)
    provenance = {
        "_type": "https://in-toto.io/Statement/v1",
        "subject": [
            {"name": item["name"], "digest": {"sha256": item["sha256"]}} for item in sbom_subjects
        ],
        "predicateType": "https://slsa.dev/provenance/v1",
        "predicate": {
            "buildDefinition": {
                "buildType": (
                    f"https://github.com/{args.repository}/.github/workflows/"
                    "release-service.yml@refs/heads/main"
                ),
                "externalParameters": {
                    "version": VERSION,
                    "releaseTag": RELEASE_TAG,
                    "image": image_reference,
                    "realKesEvidenceSha256": _digest(output / "real-kes-acceptance.json"),
                    "realKesEvidenceExecutionContext": "independent-licensed-kes",
                    "realKesEvidenceRanOnGitHubHosted": False,
                },
                "internalParameters": {},
                "resolvedDependencies": [
                    {
                        "uri": f"git+https://github.com/{args.repository}@{args.source_revision}",
                        "digest": {"gitCommit": args.source_revision},
                    },
                    {"uri": f"pkg:oci/{args.image}@{args.image_digest}"},
                    {"uri": f"pkg:oci/{KES_IMAGE}"},
                    {
                        "uri": "pkg:pypi/juntai-iam@1.1.0",
                        "digest": {"sha256": IAM_WHEEL_SHA256},
                    },
                    {
                        "uri": "pkg:pypi/juntai-iam-contracts@1.1.1",
                        "digest": {"sha256": IAM_CONTRACTS_WHEEL_SHA256},
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
    _write(output / "release.intoto.jsonl", provenance)
    checksum_assets = [path for path in sorted(output.iterdir()) if path.name != "SHA256SUMS"]
    (output / "SHA256SUMS").write_text(
        "".join(f"{_digest(path)}  {path.name}\n" for path in checksum_assets),
        encoding="utf-8",
        newline="\n",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
