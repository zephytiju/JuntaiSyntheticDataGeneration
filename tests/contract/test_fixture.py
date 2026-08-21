from __future__ import annotations

import hashlib
import json
from pathlib import Path

from juntai_synthetic_data.contracts.models import CreateGenerationRequest, canonical_json
from juntai_synthetic_data.providers import DeterministicTabularProvider


def test_published_fixture_is_self_verifying_and_reproducible() -> None:
    fixture_dir = Path("fixtures")
    request = CreateGenerationRequest.model_validate_json(
        (fixture_dir / "generation-request.v1.json").read_text()
    )
    records = json.loads((fixture_dir / "generation-records.v1.json").read_text())
    regenerated = DeterministicTabularProvider().generate(
        request.generation_contract,
        request.seed,
    )

    assert records == json.loads(canonical_json(regenerated.records))
    assert request.generation_contract.records[0].destination.schema_name == "preview_core"
    assert request.generation_contract.records[0].destination.key_fields == ("entity_id",)
    expected_sums = {
        line.split("  ", 1)[1]: line.split("  ", 1)[0]
        for line in (fixture_dir / "SHA256SUMS").read_text().splitlines()
    }
    assert set(expected_sums) == {"generation-request.v1.json", "generation-records.v1.json"}
    for name, expected_digest in expected_sums.items():
        assert hashlib.sha256((fixture_dir / name).read_bytes()).hexdigest() == expected_digest
