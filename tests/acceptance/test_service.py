from __future__ import annotations

import copy

import pytest
from conftest import make_repository, make_service, request_data
from fastapi import Request
from fastapi.testclient import TestClient
from juntai.sdk.fuse_api.adapters.http import HTTPAdapter

from juntai_synthetic_data.api import build_generation_group
from juntai_synthetic_data.contracts.models import CreateGenerationRequest, GenerationState
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.persistence import ForeignKeyDefinition, TableDefinition


class _TenantAuthorizer:
    async def authorize(
        self,
        request: Request,
        *,
        action: str,
        generation_id: str | None = None,
    ) -> str:
        del request, action, generation_id
        return "tenant-a"


def test_http_api_is_synchronous_recoverable_and_deletable() -> None:
    app = HTTPAdapter(title="Synthetic", version="1.3.0").build(
        [build_generation_group(make_service(), _TenantAuthorizer())],
        [],
    )
    client = TestClient(app)
    headers = {"Idempotency-Key": "http-key"}

    created = client.post("/v1/generations", headers=headers, json=request_data())
    assert created.status_code == 201
    generation_id = created.json()["generation_id"]
    replay = client.post("/v1/generations", headers=headers, json=request_data())
    assert replay.status_code == 200
    assert replay.json() == created.json()
    assert client.get(f"/v1/generations/{generation_id}").json() == created.json()
    deleted = client.delete(f"/v1/generations/{generation_id}")
    assert deleted.status_code == 200
    assert deleted.json()["state"] == "DELETED"


def test_synchronous_generation_commits_both_application_schemas_atomically() -> None:
    repository = make_repository()
    service = make_service(repository=repository)
    request = CreateGenerationRequest.model_validate(request_data())

    outcome = service.create_generation("tenant-a", "acceptance-1", request)

    assert outcome.replayed is False
    assert outcome.result.state is GenerationState.COMMITTED
    assert outcome.result.record_count == 9
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 3
    assert len(repository.table_rows("tenant-a", "lattice_preview", "asset")) == 6
    sites = {row["site_id"] for row in repository.table_rows("tenant-a", "axiom_preview", "site")}
    assert all(
        row["site_id"] in sites
        for row in repository.table_rows("tenant-a", "lattice_preview", "asset")
    )


def test_idempotent_replay_and_lost_response_recovery_return_exact_commit() -> None:
    repository = make_repository()
    service = make_service(repository=repository)
    request = CreateGenerationRequest.model_validate(request_data())

    first = service.create_generation("tenant-a", "recovery-key", request)
    replay = service.create_generation("tenant-a", "recovery-key", request)
    recovered = service.get_generation("tenant-a", first.result.generation_id)

    assert replay.replayed is True
    assert replay.result == first.result == recovered
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 3
    assert len(repository.table_rows("tenant-a", "lattice_preview", "asset")) == 6


def test_changed_request_with_same_key_fails_without_writes() -> None:
    repository = make_repository()
    service = make_service(repository=repository)
    first = CreateGenerationRequest.model_validate(request_data())
    changed_data = request_data()
    changed_data["seed"] = "changed"
    changed = CreateGenerationRequest.model_validate(changed_data)
    service.create_generation("tenant-a", "same-key", first)

    with pytest.raises(SyntheticDataError) as captured:
        service.create_generation("tenant-a", "same-key", changed)

    assert captured.value.code is ErrorCode.IDEMPOTENCY_KEY_REUSED
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 3


def test_destination_collision_rolls_back_cross_schema_request() -> None:
    repository = make_repository()
    request = CreateGenerationRequest.model_validate(request_data())
    generated = (
        make_service(repository=repository)
        .providers.select(request)
        .generate(request.generation_contract, request.seed)
    )
    site_id = generated.records["site"][0]["site_id"]
    repository.seed_rows(
        "tenant-a",
        "axiom_preview",
        "site",
        ({"site_id": site_id, "display_name": "existing"},),
    )
    service = make_service(repository=repository)

    with pytest.raises(SyntheticDataError) as captured:
        service.create_generation("tenant-a", "collision", request)

    assert captured.value.code is ErrorCode.DESTINATION_CONFLICT
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 1
    assert repository.table_rows("tenant-a", "lattice_preview", "asset") == ()


def test_delete_removes_only_exact_written_rows_and_is_idempotent() -> None:
    repository = make_repository()
    repository.seed_rows(
        "tenant-a",
        "axiom_preview",
        "site",
        ({"site_id": "preexisting", "display_name": "keep"},),
    )
    service = make_service(repository=repository)
    request = CreateGenerationRequest.model_validate(request_data())
    created = service.create_generation("tenant-a", "delete-key", request).result

    first = service.delete_generation("tenant-a", created.generation_id)
    replay = service.delete_generation("tenant-a", created.generation_id)

    assert first == replay
    assert first.state is GenerationState.DELETED
    assert repository.table_rows("tenant-a", "axiom_preview", "site") == (
        {"site_id": "preexisting", "display_name": "keep"},
    )
    assert repository.table_rows("tenant-a", "lattice_preview", "asset") == ()


def test_delete_conflict_rolls_back_every_deletion() -> None:
    repository = make_repository()
    service = make_service(repository=repository)
    request = CreateGenerationRequest.model_validate(request_data())
    created = service.create_generation("tenant-a", "delete-conflict", request).result
    site = repository.table_rows("tenant-a", "axiom_preview", "site")[0]
    repository.catalog[("lattice_preview", "later_reference")] = TableDefinition(
        columns={"reference_id": request.generation_contract.records[0].fields["site_id"].type},
        unique_keys=(("reference_id",),),
        required_columns=frozenset({"reference_id"}),
        foreign_keys=(
            ForeignKeyDefinition(
                columns=("reference_id",),
                target_schema="axiom_preview",
                target_table="site",
                target_columns=("site_id",),
            ),
        ),
    )
    repository.seed_rows(
        "tenant-a",
        "lattice_preview",
        "later_reference",
        ({"reference_id": site["site_id"]},),
    )

    with pytest.raises(SyntheticDataError) as captured:
        service.delete_generation("tenant-a", created.generation_id)

    assert captured.value.code is ErrorCode.DELETE_CONFLICT
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 3
    assert len(repository.table_rows("tenant-a", "lattice_preview", "asset")) == 6
    assert service.get_generation("tenant-a", created.generation_id).state is (
        GenerationState.COMMITTED
    )


def test_tenant_identity_scopes_rows_metadata_and_idempotency() -> None:
    repository = make_repository()
    service = make_service(repository=repository)
    request = CreateGenerationRequest.model_validate(request_data())
    first = service.create_generation("tenant-a", "shared-key", request).result
    second = service.create_generation("tenant-b", "shared-key", request).result

    assert first.generation_id != second.generation_id
    assert len(repository.table_rows("tenant-a", "axiom_preview", "site")) == 3
    assert len(repository.table_rows("tenant-b", "axiom_preview", "site")) == 3
    with pytest.raises(SyntheticDataError) as captured:
        service.get_generation("tenant-b", first.generation_id)
    assert captured.value.code is ErrorCode.GENERATION_NOT_FOUND


def test_database_rejects_unknown_requester_destination_atomically() -> None:
    repository = make_repository()
    data = copy.deepcopy(request_data())
    data["generation_contract"]["records"][0]["destination"]["schema"] = "platform"
    request = CreateGenerationRequest.model_validate(data)

    with pytest.raises(SyntheticDataError) as captured:
        make_service(repository=repository).create_generation("tenant-a", "unknown", request)

    assert captured.value.code is ErrorCode.DESTINATION_INVALID
    assert repository.table_rows("tenant-a", "axiom_preview", "site") == ()
    assert repository.table_rows("tenant-a", "lattice_preview", "asset") == ()


def test_synchronous_deadline_failure_leaves_no_rows() -> None:
    repository = make_repository()
    ticks = iter((0.0, 301.0))
    service = make_service(repository=repository, monotonic=lambda: next(ticks))
    request = CreateGenerationRequest.model_validate(request_data())

    with pytest.raises(SyntheticDataError) as captured:
        service.create_generation("tenant-a", "deadline", request)

    assert captured.value.code is ErrorCode.DEPENDENCY_UNAVAILABLE
    assert repository.table_rows("tenant-a", "axiom_preview", "site") == ()
