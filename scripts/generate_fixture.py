from __future__ import annotations

import hashlib
from pathlib import Path

from juntai_synthetic_data.contracts.models import GenerationContract, canonical_json
from juntai_synthetic_data.dataset import BoundedDatasetSink
from juntai_synthetic_data.providers import (
    DeterministicTabularProvider,
    GenerationExecutionContext,
)

FIXTURE_SEED = "juntai-generic-fixture-v1"
FIXTURE_IMAGE_DIGEST = "sha256:" + hashlib.sha256(b"fixture-worker-v1").hexdigest()
FIXTURE_DIR = Path("fixtures")


def fixture_contract() -> GenerationContract:
    return GenerationContract.model_validate(
        {
            "records": [
                {
                    "record_type": "entity",
                    "count": {"maximum": 5},
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
            "bounds": {"max_records": 5, "max_bytes": 16_384, "max_shards": 1},
            "output": {"format": "jsonl", "compression": "none"},
        }
    )


def main() -> None:
    contract = fixture_contract()
    provider = DeterministicTabularProvider(worker_image_digest=FIXTURE_IMAGE_DIGEST)
    with BoundedDatasetSink(contract) as sink:
        output = provider.generate(
            contract,
            FIXTURE_SEED,
            sink,
            GenerationExecutionContext("fixture", "fixture", lambda: False, 30),
        )

    FIXTURE_DIR.mkdir(parents=True, exist_ok=True)
    shard = output.shards[0]
    dataset_path = FIXTURE_DIR / "generic-dataset.v1.jsonl"
    manifest_path = FIXTURE_DIR / "generic-dataset.v1.manifest.json"
    sums_path = FIXTURE_DIR / "SHA256SUMS"
    dataset_path.write_bytes(shard.data)
    manifest = {
        "schema_version": "juntai.synthetic-data.fixture/v1",
        "seed": FIXTURE_SEED,
        "contract_digest": contract.digest,
        "logical_dataset_digest": output.logical_digest,
        "record_count": output.record_count,
        "byte_count": output.byte_count,
        "shards": [
            {
                "name": dataset_path.name,
                "digest": shard.digest,
                "media_type": shard.media_type,
                "record_count": shard.record_count,
            }
        ],
    }
    manifest_path.write_bytes(canonical_json(manifest) + b"\n")
    sums = [
        f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}"
        for path in (dataset_path, manifest_path)
    ]
    sums_path.write_text("\n".join(sums) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
