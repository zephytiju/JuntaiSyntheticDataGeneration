from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor

import pytest
from conftest import catalog, make_repository, make_service, request_data

from juntai_synthetic_data.contracts.models import CreateGenerationRequest, FieldType
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


def test_caller_declared_non_unique_key_is_not_preflighted() -> None:
    definitions = catalog()
    definitions[("axiom_preview", "site")] = TableDefinition(
        columns={"site_id": FieldType.STRING, "display_name": FieldType.STRING},
        unique_keys=(("display_name",),),
        required_columns=frozenset({"site_id", "display_name"}),
    )
    repository = InMemoryGenerationRepository(definitions)

    outcome = make_service(repository=repository).create_generation(
        "tenant-a", "caller-key", CreateGenerationRequest.model_validate(request_data())
    )

    assert outcome.result.record_count == 9


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


def test_duplicate_caller_key_causes_atomic_delete_conflict() -> None:
    repository = make_repository()
    service = make_service(repository=repository)
    created = service.create_generation(
        "tenant-a", "duplicate-key", CreateGenerationRequest.model_validate(request_data())
    ).result
    sites = list(repository.table_rows("tenant-a", "axiom_preview", "site"))
    sites.append(dict(sites[0]))
    repository.seed_rows("tenant-a", "axiom_preview", "site", tuple(sites))

    with pytest.raises(SyntheticDataError) as captured:
        service.delete_generation("tenant-a", created.generation_id)

    assert captured.value.code is ErrorCode.DELETE_CONFLICT
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 4
    assert len(repository.table_rows("tenant-a", "lattice_preview", "asset")) == 6
