from __future__ import annotations

import argparse
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

import psycopg
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from juntai_synthetic_data.contracts.models import CreateGenerationRequest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.migration import (
    Migration,
    MigrationBinding,
    MigrationDatabaseError,
    apply_migrations,
    load_migrations,
    read_dsn_file,
)
from juntai_synthetic_data.persistence import SqlGenerationRepository
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.providers import DeterministicTabularProvider, ProviderRegistry
from juntai_synthetic_data.service import SyntheticDataService

RUNTIME_ROLE = "synthetic_runtime_acceptance"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dsn-file", required=True)
    parser.add_argument("--phase", choices=("primary", "post-restart"), required=True)
    return parser


def _binding() -> MigrationBinding:
    return MigrationBinding(
        source_revision=os.environ["JUNTAI_SOURCE_REVISION"],
        image_digest=os.environ["JUNTAI_SERVICE_IMAGE_DIGEST"],
    )


def _execute(dsn: str, statement: str, parameters: tuple[Any, ...] = ()) -> list[tuple[Any, ...]]:
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute(statement, parameters)
        return cursor.fetchall() if cursor.description else []


def _reset(dsn: str) -> None:
    _execute(dsn, "DROP SCHEMA IF EXISTS juntai_synthetic_data CASCADE")


def _empty_repeat_and_check(dsn: str, binding: MigrationBinding) -> None:
    _reset(dsn)
    first = apply_migrations(dsn, binding)
    second = apply_migrations(dsn, binding)
    checked = apply_migrations(dsn, binding, check=True)
    expected = tuple(item.migration_id for item in load_migrations())
    assert first.applied == expected
    assert second.status == "current"
    assert checked.status == "current"


def _concurrency(dsn: str, binding: MigrationBinding) -> None:
    _reset(dsn)
    expected = tuple(item.migration_id for item in load_migrations())
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: apply_migrations(dsn, binding), range(4)))
    assert sum(result.applied == expected for result in results) == 1
    assert sum(result.status == "current" for result in results) == 3


def _partial_failure(dsn: str, binding: MigrationBinding) -> None:
    _reset(dsn)
    baseline = load_migrations()[0]
    failing_sql = "CREATE TABLE juntai_synthetic_data.must_rollback (id integer); SELECT 1 / 0"
    failing = Migration(
        migration_id="0002_forced_failure",
        path="acceptance-only",
        checksum=hashlib.sha256(failing_sql.encode()).hexdigest(),
        sql=failing_sql,
    )
    try:
        apply_migrations(dsn, binding, migrations=(baseline, failing))
    except MigrationDatabaseError:
        pass
    else:
        raise AssertionError("forced partial failure did not fail")
    assert _execute(dsn, "SELECT to_regclass('juntai_synthetic_data.jobs')") == [(None,)]
    assert _execute(dsn, "SELECT to_regclass('juntai_synthetic_data.must_rollback')") == [(None,)]
    assert apply_migrations(dsn, binding).applied == tuple(
        item.migration_id for item in load_migrations()
    )


def _released_baseline_upgrade(dsn: str, binding: MigrationBinding) -> None:
    _reset(dsn)
    historical = load_migrations()[:2]
    released = MigrationBinding(
        source_revision="b67c6fe17e94f62856c421ebd1cddebcea2e5540",
        image_digest="sha256:" + "8" * 64,
        service_version="1.2.0",
    )
    assert apply_migrations(dsn, released, migrations=historical).applied == (
        "0001_jobs",
        "0002_worker_protocol",
    )
    result = apply_migrations(dsn, binding)
    assert result.applied == ("0003_synchronous_generations",)
    assert _execute(dsn, "SELECT to_regclass('juntai_synthetic_data.jobs')") == [(None,)]


def _runtime_dsn(admin_dsn: str) -> str:
    settings = conninfo_to_dict(admin_dsn)
    password = str(settings["password"])
    with psycopg.connect(admin_dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (RUNTIME_ROLE,))
        if cursor.fetchone() is None:
            cursor.execute(
                sql.SQL("CREATE USER {} PASSWORD {}").format(
                    sql.Identifier(RUNTIME_ROLE),
                    sql.Literal(password),
                )
            )
        else:
            cursor.execute(
                sql.SQL("ALTER USER {} PASSWORD {}").format(
                    sql.Identifier(RUNTIME_ROLE),
                    sql.Literal(password),
                )
            )
    settings["user"] = RUNTIME_ROLE
    return make_conninfo(**settings)


def _prepare_application_schema(admin_dsn: str) -> None:
    with psycopg.connect(admin_dsn) as connection, connection.cursor() as cursor:
        cursor.execute("DROP SCHEMA IF EXISTS lattice_preview CASCADE")
        cursor.execute("DROP SCHEMA IF EXISTS axiom_preview CASCADE")
        cursor.execute('DROP SCHEMA IF EXISTS "Caller Preview" CASCADE')
        cursor.execute("CREATE SCHEMA axiom_preview")
        cursor.execute("CREATE SCHEMA lattice_preview")
        cursor.execute('CREATE SCHEMA "Caller Preview"')
        cursor.execute(
            """
            CREATE TABLE axiom_preview.site (
                tenant_id varchar(128) NOT NULL,
                site_id varchar(64) NOT NULL UNIQUE,
                display_name varchar(200) NOT NULL,
                PRIMARY KEY (tenant_id, site_id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE lattice_preview.asset (
                tenant_id varchar(128) NOT NULL,
                asset_id varchar(64) NOT NULL UNIQUE,
                site_id varchar(64) NOT NULL,
                reading double precision NOT NULL,
                PRIMARY KEY (tenant_id, asset_id),
                FOREIGN KEY (site_id) REFERENCES axiom_preview.site(site_id)
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE "Caller Preview"."Quoted Table" (
                "Tenant ID" varchar(128) NOT NULL,
                "Row ID" varchar(64) NOT NULL,
                PRIMARY KEY ("Tenant ID", "Row ID")
            )
            """
        )
        for table in ("axiom_preview.site", "lattice_preview.asset"):
            cursor.execute(f"ALTER TABLE {table} ENABLE ROW LEVEL SECURITY")
            cursor.execute(f"ALTER TABLE {table} FORCE ROW LEVEL SECURITY")
        cursor.execute('ALTER TABLE "Caller Preview"."Quoted Table" ENABLE ROW LEVEL SECURITY')
        cursor.execute('ALTER TABLE "Caller Preview"."Quoted Table" FORCE ROW LEVEL SECURITY')
        cursor.execute(
            "CREATE POLICY site_tenant ON axiom_preview.site "
            "USING (tenant_id = current_setting('juntai.tenant_id', true)) "
            "WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true))"
        )
        cursor.execute(
            "CREATE POLICY asset_tenant ON lattice_preview.asset "
            "USING (tenant_id = current_setting('juntai.tenant_id', true)) "
            "WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true))"
        )
        cursor.execute(
            'CREATE POLICY quoted_tenant ON "Caller Preview"."Quoted Table" '
            "USING (\"Tenant ID\" = current_setting('juntai.tenant_id', true)) "
            "WITH CHECK (\"Tenant ID\" = current_setting('juntai.tenant_id', true))"
        )
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA axiom_preview, lattice_preview TO {}").format(
                sql.Identifier(RUNTIME_ROLE)
            )
        )
        cursor.execute(
            sql.SQL('GRANT USAGE ON SCHEMA "Caller Preview" TO {}').format(
                sql.Identifier(RUNTIME_ROLE)
            )
        )
        cursor.execute(
            sql.SQL(
                "GRANT SELECT, INSERT, DELETE ON axiom_preview.site, lattice_preview.asset TO {}"
            ).format(sql.Identifier(RUNTIME_ROLE))
        )
        cursor.execute(
            sql.SQL('GRANT SELECT, INSERT, DELETE ON "Caller Preview"."Quoted Table" TO {}').format(
                sql.Identifier(RUNTIME_ROLE)
            )
        )
        cursor.execute(
            sql.SQL("GRANT USAGE ON SCHEMA juntai_synthetic_data TO {}").format(
                sql.Identifier(RUNTIME_ROLE)
            )
        )
        cursor.execute(
            sql.SQL(
                "GRANT SELECT, INSERT, UPDATE ON juntai_synthetic_data.generations, "
                "juntai_synthetic_data.generation_rows TO {}"
            ).format(sql.Identifier(RUNTIME_ROLE))
        )


def _request(tenant_id: str, seed: str) -> CreateGenerationRequest:
    return CreateGenerationRequest.model_validate(
        {
            "generation_contract": {
                "records": [
                    {
                        "record_type": "site",
                        "count": 2,
                        "destination": {
                            "schema": "axiom_preview",
                            "table": "site",
                            "columns": {
                                "tenant_id": "tenant_id",
                                "site_id": "site_id",
                                "name": "display_name",
                            },
                            "key_fields": ["site_id"],
                        },
                        "fields": {
                            "tenant_id": {
                                "type": "string",
                                "distribution": {"kind": "constant", "value": tenant_id},
                            },
                            "site_id": {
                                "type": "string",
                                "unique": True,
                                "distribution": {"kind": "uuid"},
                            },
                            "name": {"type": "string"},
                        },
                    },
                    {
                        "record_type": "asset",
                        "count": 4,
                        "destination": {
                            "schema": "lattice_preview",
                            "table": "asset",
                            "columns": {
                                "tenant_id": "tenant_id",
                                "asset_id": "asset_id",
                                "site_id": "site_id",
                                "reading": "reading",
                            },
                            "key_fields": ["asset_id"],
                        },
                        "fields": {
                            "tenant_id": {
                                "type": "string",
                                "distribution": {"kind": "constant", "value": tenant_id},
                            },
                            "asset_id": {"type": "string", "unique": True},
                            "site_id": {"type": "string"},
                            "reading": {"type": "number"},
                        },
                    },
                ],
                "relations": [{"from": "asset.site_id", "to": "site.site_id", "required": True}],
                "bounds": {"max_records": 6, "max_bytes": 1_000_000},
            },
            "seed": seed,
            "provider": {"class": "tabular", "requirements": {"deterministic": True}},
            "policy": {"data_classification": "synthetic"},
        }
    )


def _quoted_destination_request(tenant_id: str) -> CreateGenerationRequest:
    return CreateGenerationRequest.model_validate(
        {
            "generation_contract": {
                "records": [
                    {
                        "record_type": "quoted",
                        "count": 2,
                        "destination": {
                            "schema": "Caller Preview",
                            "table": "Quoted Table",
                            "columns": {"tenant_id": "Tenant ID", "row_id": "Row ID"},
                            "key_fields": ["row_id"],
                        },
                        "fields": {
                            "tenant_id": {
                                "type": "string",
                                "distribution": {"kind": "constant", "value": tenant_id},
                            },
                            "row_id": {"type": "string", "unique": True},
                        },
                    }
                ],
                "bounds": {"max_records": 2, "max_bytes": 100_000},
            },
            "seed": "quoted-destination-seed",
            "provider": {"class": "tabular", "requirements": {"deterministic": True}},
            "policy": {"data_classification": "synthetic"},
        }
    )


def _service(runtime_dsn: str) -> SyntheticDataService:
    repository = SqlGenerationRepository(
        lambda: psycopg.connect(runtime_dsn),
        retry_base_seconds=0,
        retry_cap_seconds=0,
    )
    return SyntheticDataService(
        repository=repository,
        providers=ProviderRegistry((DeterministicTabularProvider(),)),
        policy=DefaultPolicyEngine(),
    )


def _generation_matrix(admin_dsn: str) -> None:
    runtime_dsn = _runtime_dsn(admin_dsn)
    _prepare_application_schema(admin_dsn)
    service = _service(runtime_dsn)
    request_a = _request("tenant-a", "real-kes-seed-a")
    created = service.create_generation("tenant-a", "real-kes-a", request_a)
    assert not created.replayed and created.result.record_count == 6
    assert _execute(admin_dsn, "SELECT count(*) FROM axiom_preview.site") == [(2,)]
    assert _execute(admin_dsn, "SELECT count(*) FROM lattice_preview.asset") == [(4,)]
    assert _execute(admin_dsn, "SELECT count(*) FROM juntai_synthetic_data.generation_rows") == [
        (6,)
    ]

    replay = _service(runtime_dsn).create_generation("tenant-a", "real-kes-a", request_a)
    assert replay.replayed and replay.result == created.result
    assert (
        _service(runtime_dsn).get_generation("tenant-a", created.result.generation_id)
        == created.result
    )

    try:
        service.create_generation("tenant-a", "collision", request_a)
    except SyntheticDataError as error:
        assert error.code is ErrorCode.DESTINATION_CONFLICT
    else:
        raise AssertionError("destination collision unexpectedly committed")
    assert _execute(
        admin_dsn,
        "SELECT count(*) FROM juntai_synthetic_data.generations WHERE tenant_id = 'tenant-a'",
    ) == [(1,)]

    invalid_data = request_a.model_dump(mode="json", by_alias=True)
    invalid_data["generation_contract"]["records"][0]["destination"]["schema"] = "Missing Schema"
    invalid_request = CreateGenerationRequest.model_validate(invalid_data)
    try:
        service.create_generation("tenant-a", "missing-destination", invalid_request)
    except SyntheticDataError as error:
        assert error.code is ErrorCode.DESTINATION_INVALID
    else:
        raise AssertionError("nonexistent caller destination unexpectedly committed")
    assert _execute(admin_dsn, "SELECT count(*) FROM axiom_preview.site") == [(2,)]
    assert _execute(admin_dsn, "SELECT count(*) FROM lattice_preview.asset") == [(4,)]
    assert _execute(
        admin_dsn,
        "SELECT count(*) FROM juntai_synthetic_data.generations WHERE tenant_id = 'tenant-a'",
    ) == [(1,)]

    quoted = service.create_generation(
        "tenant-a", "quoted-destination", _quoted_destination_request("tenant-a")
    )
    assert quoted.result.record_count == 2
    assert _execute(admin_dsn, 'SELECT count(*) FROM "Caller Preview"."Quoted Table"') == [(2,)]
    assert (
        service.delete_generation("tenant-a", quoted.result.generation_id).state.value == "DELETED"
    )
    assert _execute(admin_dsn, 'SELECT count(*) FROM "Caller Preview"."Quoted Table"') == [(0,)]

    request_b = _request("tenant-b", "real-kes-seed-b")
    created_b = service.create_generation("tenant-b", "real-kes-b", request_b)
    assert created_b.result.record_count == 6
    with psycopg.connect(runtime_dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT set_config('juntai.tenant_id', 'tenant-a', false)")
        cursor.execute("SELECT count(*) FROM juntai_synthetic_data.generations")
        assert cursor.fetchone() == (2,)
        cursor.execute("SELECT count(*) FROM axiom_preview.site")
        assert cursor.fetchone() == (2,)
        cursor.execute("SELECT set_config('juntai.tenant_id', 'tenant-b', false)")
        cursor.execute("SELECT count(*) FROM juntai_synthetic_data.generations")
        assert cursor.fetchone() == (1,)
        cursor.execute("SELECT count(*) FROM lattice_preview.asset")
        assert cursor.fetchone() == (4,)

    deleted = service.delete_generation("tenant-b", created_b.result.generation_id)
    assert deleted.state.value == "DELETED"
    assert service.delete_generation("tenant-b", created_b.result.generation_id) == deleted
    assert _execute(admin_dsn, "SELECT count(*) FROM axiom_preview.site") == [(2,)]
    assert _execute(admin_dsn, "SELECT count(*) FROM lattice_preview.asset") == [(4,)]


def _primary(dsn: str, binding: MigrationBinding) -> dict[str, object]:
    _empty_repeat_and_check(dsn, binding)
    _concurrency(dsn, binding)
    _partial_failure(dsn, binding)
    _released_baseline_upgrade(dsn, binding)
    _generation_matrix(dsn)
    return {
        "phase": "primary",
        "checks": [
            "empty-database",
            "repeat-idempotence",
            "concurrency-lock",
            "transactional-partial-failure",
            "transactional-failure-recovery",
            "released-1.2.0-baseline-upgrade",
            "cross-schema-atomic-write",
            "idempotent-replay",
            "lost-response-recovery",
            "destination-conflict-rollback",
            "database-destination-rejection",
            "quoted-caller-destination",
            "exact-key-delete",
            "delete-idempotence",
            "tenant-rls-isolation",
            "no-platform-database-dependency",
        ],
    }


def _post_restart(dsn: str, binding: MigrationBinding) -> dict[str, object]:
    result = apply_migrations(dsn, binding)
    assert result.status == "current"
    assert result.current == tuple(item.migration_id for item in load_migrations())
    runtime_dsn = _runtime_dsn(dsn)
    rows = _execute(
        dsn,
        "SELECT generation_id FROM juntai_synthetic_data.generations "
        "WHERE tenant_id = 'tenant-a' AND state = 'COMMITTED'",
    )
    assert len(rows) == 1
    recovered = _service(runtime_dsn).get_generation("tenant-a", str(rows[0][0]))
    assert recovered.state.value == "COMMITTED"
    return {"phase": "post-restart", "checks": ["database-restart", "ledger-current"]}


def main() -> None:
    args = _parser().parse_args()
    dsn = read_dsn_file(args.dsn_file)
    binding = _binding()
    evidence = _primary(dsn, binding) if args.phase == "primary" else _post_restart(dsn, binding)
    evidence.update(
        {
            "schemaVersion": "juntai.synthetic-data.real-kes-acceptance/v1",
            "sourceRevision": binding.source_revision,
            "serviceImageDigest": binding.image_digest,
            "migrationIds": [item.migration_id for item in load_migrations()],
            "databaseVersion": _execute(dsn, "SELECT version()")[-1][0],
        }
    )
    print(json.dumps(evidence, sort_keys=True))


if __name__ == "__main__":
    main()
