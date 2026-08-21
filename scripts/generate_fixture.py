from __future__ import annotations

import hashlib
from pathlib import Path

from juntai_synthetic_data.contracts.models import CreateGenerationRequest, canonical_json
from juntai_synthetic_data.providers import DeterministicTabularProvider

FIXTURE_SEED = "juntai-generic-fixture-v1"
FIXTURE_DIR = Path("fixtures")


def fixture_request() -> CreateGenerationRequest:
    return CreateGenerationRequest.model_validate(
        {
            "generation_contract": {
                "records": [
                    {
                        "record_type": "entity",
                        "count": 5,
                        "destination": {
                            "schema": "preview_core",
                            "table": "entity",
                            "columns": {
                                "entity_id": "entity_id",
                                "enabled": "enabled",
                                "ordinal": "ordinal",
                            },
                            "key_fields": ["entity_id"],
                        },
                        "fields": {
                            "enabled": {"type": "boolean"},
                            "entity_id": {
                                "type": "string",
                                "unique": True,
                                "distribution": {"kind": "uuid"},
                            },
                            "ordinal": {
                                "type": "integer",
                                "distribution": {"kind": "sequence", "start": 1, "step": 1},
                            },
                        },
                    }
                ],
                "bounds": {"max_records": 5, "max_bytes": 16_384},
            },
            "seed": FIXTURE_SEED,
            "provider": {"class": "tabular", "requirements": {"deterministic": True}},
            "policy": {"data_classification": "synthetic"},
        }
    )


def main() -> None:
    request = fixture_request()
    dataset = DeterministicTabularProvider().generate(
        request.generation_contract,
        request.seed,
    )
    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    request_path = FIXTURE_DIR / "generation-request.v1.json"
    records_path = FIXTURE_DIR / "generation-records.v1.json"
    request_path.write_bytes(canonical_json(request) + b"\n")
    records_path.write_bytes(canonical_json(dataset.records) + b"\n")
    sums = [
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}"
        for path in (request_path, records_path)
    ]
    (FIXTURE_DIR / "SHA256SUMS").write_text("\n".join(sums) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
