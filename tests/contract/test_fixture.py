from __future__ import annotations

import hashlib
import json
from pathlib import Path


def test_published_fixture_is_self_verifying_and_generic() -> None:
    fixture_dir = Path("fixtures")
    manifest = json.loads((fixture_dir / "generic-dataset.v1.manifest.json").read_text())
    dataset = (fixture_dir / "generic-dataset.v1.jsonl").read_bytes()

    assert manifest["schema_version"] == "juntai.synthetic-data.fixture/v1"
    assert manifest["record_count"] == 5
    assert manifest["shards"][0]["digest"] == f"sha256:{hashlib.sha256(dataset).hexdigest()}"
    assert {json.loads(line)["record_type"] for line in dataset.splitlines()} == {"entity"}

    expected_sums = {
        line.split("  ", 1)[1]: line.split("  ", 1)[0]
        for line in (fixture_dir / "SHA256SUMS").read_text().splitlines()
    }
    for name, expected_digest in expected_sums.items():
        assert hashlib.sha256((fixture_dir / name).read_bytes()).hexdigest() == expected_digest
