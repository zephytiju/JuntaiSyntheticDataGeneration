from __future__ import annotations

import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).parents[2]
SQL_DIGEST = "af29058d1ca61516415cc3b3f877987012c371fba5fdec0170bc83dc76c19822"
SWP_SQL_DIGEST = "3e1938165b6ff0bcc9dcfc80288e74f32715474e259f82b346307724c0809779"
KES_DIGEST = "0bce318e74adca7a3d619b55b336269017507fd679833b7ce5d8400289661724"


def test_migration_set_is_exact_ordered_and_service_owned() -> None:
    document = json.loads((ROOT / "migrations" / "manifest.v1.json").read_text())
    sql = (ROOT / "migrations" / "0001_jobs.sql").read_bytes()

    assert [item["id"] for item in document["migrations"]] == [
        "0001_jobs",
        "0002_worker_protocol",
    ]
    assert hashlib.sha256(sql).hexdigest() == SQL_DIGEST
    assert document["migrations"][0]["sha256"] == SQL_DIGEST
    assert document["migrations"][1]["sha256"] == SWP_SQL_DIGEST
    assert (
        hashlib.sha256((ROOT / "migrations" / "0002_worker_protocol.sql").read_bytes()).hexdigest()
        == SWP_SQL_DIGEST
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
        "_tenant_isolation",
        "_api_worker_startup",
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


def test_service_release_requires_real_kes_evidence_and_attestation() -> None:
    workflow = (ROOT / ".github" / "workflows" / "release-service.yml").read_text()

    assert 'tags: ["synthetic-data-v1.2.0"]' in workflow
    assert "REAL_KES_EVIDENCE_BASE64=" in workflow
    assert "actions/attest-build-provenance@v3" in workflow
    assert "generate_service_release.py" in workflow
    assert "isImmutable" in workflow
