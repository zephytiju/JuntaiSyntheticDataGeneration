from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from conftest import catalog, make_repository, make_service, request_data

from juntai_synthetic_data.contracts.models import CreateGenerationRequest, FieldType
from juntai_synthetic_data.destinations import ALLOWLIST_VERSION, DestinationAllowlist
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.persistence import InMemoryGenerationRepository, TableDefinition


def test_concurrent_identical_requests_commit_once() -> None:
    repository = make_repository()
    service = make_service(repository=repository)
    request = CreateGenerationRequest.model_validate(request_data())

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(
            executor.map(
                lambda _: service.create_generation("tenant-a", "concurrent-key", request),
                range(2),
            )
        )

    assert {outcome.result.generation_id for outcome in outcomes} == {
        outcomes[0].result.generation_id
    }
    assert sorted(outcome.replayed for outcome in outcomes) == [False, True]
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 3
    assert len(repository.table_rows("tenant-a", "lattice_preview", "asset")) == 6


def test_written_key_ledger_is_canonical_and_bounded() -> None:
    repository = make_repository()
    result = (
        make_service(repository=repository)
        .create_generation(
            "tenant-a", "ledger", CreateGenerationRequest.model_validate(request_data())
        )
        .result
    )
    keys = repository.ledger_bytes("tenant-a", result.generation_id)

    assert len(keys) == 9
    assert all(
        key == json.dumps(json.loads(key), sort_keys=True, separators=(",", ":")).encode()
        for key in keys
    )
    assert max(map(len, keys)) < 8192


def test_non_unique_destination_key_is_rejected_before_generation() -> None:
    definitions = catalog()
    definitions[("axiom_preview", "site")] = TableDefinition(
        columns={"site_id": FieldType.STRING, "display_name": FieldType.STRING},
        unique_keys=(("display_name",),),
        required_columns=frozenset({"site_id", "display_name"}),
    )
    repository = InMemoryGenerationRepository(definitions)

    with pytest.raises(SyntheticDataError) as captured:
        make_service(repository=repository).create_generation(
            "tenant-a", "invalid-key", CreateGenerationRequest.model_validate(request_data())
        )

    assert captured.value.code is ErrorCode.DESTINATION_INVALID


def test_missing_generated_row_causes_atomic_delete_conflict() -> None:
    repository = make_repository()
    service = make_service(repository=repository)
    created = service.create_generation(
        "tenant-a", "missing-row", CreateGenerationRequest.model_validate(request_data())
    ).result
    current = list(repository.table_rows("tenant-a", "lattice_preview", "asset"))
    repository.seed_rows("tenant-a", "lattice_preview", "asset", tuple(current[1:]))

    with pytest.raises(SyntheticDataError) as captured:
        service.delete_generation("tenant-a", created.generation_id)

    assert captured.value.code is ErrorCode.DELETE_CONFLICT
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 3
    assert len(repository.table_rows("tenant-a", "lattice_preview", "asset")) == 5


def test_allowlist_file_is_exact_and_rejects_unknown_shape(tmp_path) -> None:
    path = tmp_path / "destinations.json"
    path.write_text(
        json.dumps(
            {
                "schemaVersion": ALLOWLIST_VERSION,
                "destinations": [
                    {"schema": "axiom_preview", "tables": ["site"]},
                    {"schema": "lattice_preview", "tables": ["asset"]},
                ],
            }
        )
    )
    allowlist = DestinationAllowlist.from_file(str(path))
    assert allowlist.allows("axiom_preview", "site")
    assert not allowlist.allows("platform", "anything")

    path.write_text(
        json.dumps({"schemaVersion": ALLOWLIST_VERSION, "destinations": [], "dsn": "x"})
    )
    with pytest.raises(ValueError, match="top-level"):
        DestinationAllowlist.from_file(str(path))
