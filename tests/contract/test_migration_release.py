from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).parents[2]
SQL_DIGEST = "af29058d1ca61516415cc3b3f877987012c371fba5fdec0170bc83dc76c19822"
SWP_SQL_DIGEST = "3e1938165b6ff0bcc9dcfc80288e74f32715474e259f82b346307724c0809779"
SYNCHRONOUS_SQL_DIGEST = "87c1133e49344322b92a885fc9c44089d0cadd1e0cce14f5b7170c6093370b11"
KES_DIGEST = "0bce318e74adca7a3d619b55b336269017507fd679833b7ce5d8400289661724"
RELEASE_SOURCE = "54ea4f8c28337714564de40f19a4f3c9124e9d20"
RELEASE_IMAGE = "e6a4c903d3fdc5f4eb315a2a50fe3e1af6f4febeb3d696a4cd900c9887921912"
RELEASE_KES_EVIDENCE = "c9042a441f4c86796ca661ba82c55c4e94f9fc7f8de032e781223584b62f07c9"


def test_migration_set_is_exact_ordered_and_service_owned() -> None:
    document = json.loads((ROOT / "migrations" / "manifest.v1.json").read_text())
    sql = (ROOT / "migrations" / "0001_jobs.sql").read_bytes()

    assert [item["id"] for item in document["migrations"]] == [
        "0001_jobs",
        "0002_worker_protocol",
        "0003_synchronous_generations",
    ]
    assert hashlib.sha256(sql).hexdigest() == SQL_DIGEST
    assert document["migrations"][0]["sha256"] == SQL_DIGEST
    assert document["migrations"][1]["sha256"] == SWP_SQL_DIGEST
    assert document["migrations"][2]["sha256"] == SYNCHRONOUS_SQL_DIGEST
    assert (
        hashlib.sha256((ROOT / "migrations" / "0002_worker_protocol.sql").read_bytes()).hexdigest()
        == SWP_SQL_DIGEST
    )
    assert (
        hashlib.sha256(
            (ROOT / "migrations" / "0003_synchronous_generations.sql").read_bytes()
        ).hexdigest()
        == SYNCHRONOUS_SQL_DIGEST
    )
    assert document["database"]["schema"] == "juntai_synthetic_data"
    assert document["command"]["entryPoint"] == "juntai-synthetic-data migrate"


def test_real_kes_harness_is_pinned_and_covers_required_matrix() -> None:
    runner = (ROOT / "scripts" / "run-real-kes-acceptance.sh").read_text()
    harness = (ROOT / "scripts" / "real_kes_acceptance.py").read_text()

    assert KES_DIGEST in runner
    for check in (
        "_empty_repeat_and_check",
        "_concurrency",
        "_partial_failure",
        "_released_baseline_upgrade",
        "_generation_matrix",
        "cross-schema-atomic-write",
        "database-destination-rejection",
        "lost-response-recovery",
        "quoted-caller-destination",
        "exact-key-delete",
        "tenant-rls-isolation",
        "post-restart",
        "transactional-failure-recovery",
    ):
        assert check in harness


def test_main_publication_never_retags_the_documented_1_0_0_image() -> None:
    publication = (ROOT / ".github" / "workflows" / "publish.yml").read_text()

    assert "juntai-synthetic-data-generation:1.0.0" not in publication
    assert "juntai-synthetic-data-generation:sha-${{ github.sha }}" in publication
    assert "provenance: mode=max" in publication
    assert "sbom: true" in publication


def test_publication_has_no_self_hosted_or_fake_hosted_kes_gate() -> None:
    workflows = "\n".join(path.read_text() for path in (ROOT / ".github/workflows").glob("*.yml"))

    assert "self-hosted" not in workflows
    assert "juntai-platform" not in workflows
    assert "licensed-acceptance" not in workflows
    assert not (ROOT / ".github/workflows/acceptance.yml").exists()


def test_exact_independent_release_kes_evidence_is_committed() -> None:
    path = ROOT / "release-evidence/synthetic-data-v1.3.0.json"
    evidence = json.loads(path.read_text())

    assert hashlib.sha256(path.read_bytes()).hexdigest() == RELEASE_KES_EVIDENCE
    assert evidence["result"] == "passed"
    assert evidence["sourceRevision"] == RELEASE_SOURCE
    assert evidence["serviceImageDigest"] == f"sha256:{RELEASE_IMAGE}"
    assert evidence["kingbaseVersion"] == "KingbaseES V009R001C010"
    assert len(evidence["checks"]) == 18


def test_service_release_requires_real_kes_evidence_and_attestation() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release-service.yml").read_text()

    assert 'paths: ["release-evidence/synthetic-data-v1.3.0.json"]' in workflow
    assert "REAL_KES_EVIDENCE_BASE64=" in workflow
    assert workflow.index("actions/attest-build-provenance@v3") < workflow.index(
        "Create or verify exact annotated release tags"
    )
    assert 'test "$(git cat-file -t "$release_ref")" = tag' in workflow
    assert 'test "$(git rev-list -n 1 "$release_ref")" = "$target"' in workflow
    assert '"Synthetic Data documentation capability 1.3.0" "$DOCUMENTATION_SHA"' in workflow
    assert "actions/attest-build-provenance@v3" in workflow
    assert "generate_service_release.py" in workflow
    assert "isImmutable" in workflow


def test_release_handoff_pins_exact_test_fleet_admission() -> None:
    generator = (ROOT / "scripts" / "generate_service_release.py").read_text()
    assert '"environmentVariable": "JUNTAI_SYNTHETIC_DATA_TEST_FLEET"' in generator
    assert '"requiredValue": "true"' in generator
    assert '"comparison": "exact"' in generator
    assert '"databaseDsnFileEnvironment": "JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE"' in generator
    assert '"executionContext": "independent-licensed-kes"' in generator
    assert '"githubHosted": False' in generator
    assert "JUNTAI_SYNTHETIC_DATA_DESTINATION_ALLOWLIST_FILE" not in generator
