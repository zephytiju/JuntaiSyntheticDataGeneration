from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import psycopg

from juntai_synthetic_data.api import build_server
from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.jobs import SqlJobRepository
from juntai_synthetic_data.migration import (
    Migration,
    MigrationBinding,
    MigrationDatabaseError,
    apply_migrations,
    load_migrations,
    read_dsn_file,
)
from juntai_synthetic_data.policy import DefaultPolicyEngine
from juntai_synthetic_data.providers import DeterministicTabularProvider, ProviderRegistry
from juntai_synthetic_data.publication import PublishedDataset
from juntai_synthetic_data.quotas import InMemoryQuotaLedger, QuotaLimits
from juntai_synthetic_data.scheduling import JobScheduler
from juntai_synthetic_data.service import SyntheticDataService

ROOT = Path(__file__).parents[1]


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
    assert first.applied == ("0001_jobs",)
    assert second.status == "current"
    assert checked.status == "current"


def _concurrency(dsn: str, binding: MigrationBinding) -> None:
    _reset(dsn)
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: apply_migrations(dsn, binding), range(4)))
    assert sum(result.applied == ("0001_jobs",) for result in results) == 1
    assert sum(result.status == "current" for result in results) == 3
    rows = _execute(
        dsn,
        "SELECT migration_id, count(*) FROM juntai_synthetic_data.schema_migrations "
        "GROUP BY migration_id",
    )
    assert rows == [("0001_jobs", 1)]


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


def _released_baseline_upgrade(dsn: str, binding: MigrationBinding) -> None:
    _reset(dsn)
    baseline_sql = (ROOT / "migrations" / "0001_jobs.sql").read_text()
    _execute(dsn, baseline_sql)
    result = apply_migrations(dsn, binding)
    assert result.adopted == ("0001_jobs",)
    rows = _execute(
        dsn,
        "SELECT migration_id, adopted_from_baseline FROM juntai_synthetic_data.schema_migrations",
    )
    assert rows == [("0001_jobs", True)]


def _tenant_isolation(dsn: str) -> None:
    role = "synthetic_rls_acceptance"
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        if cursor.fetchone() is None:
            cursor.execute(f"CREATE ROLE {role} NOLOGIN")
        cursor.execute(f"GRANT USAGE ON SCHEMA juntai_synthetic_data TO {role}")
        cursor.execute(f"GRANT SELECT ON juntai_synthetic_data.jobs TO {role}")
        for tenant, job in (("tenant-a", "job_a"), ("tenant-b", "job_b")):
            cursor.execute(
                """
                INSERT INTO juntai_synthetic_data.jobs (
                    job_id, tenant_id, idempotency_key, request_digest, request_json,
                    state, version, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, '{}'::jsonb, 'ACCEPTED', 1,
                          CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                (job, tenant, f"key-{tenant}", "sha256:" + "1" * 64),
            )
        cursor.execute(f"SET ROLE {role}")
        cursor.execute("SELECT set_config('juntai.tenant_id', 'tenant-a', false)")
        cursor.execute("SELECT tenant_id, job_id FROM juntai_synthetic_data.jobs ORDER BY job_id")
        assert cursor.fetchall() == [("tenant-a", "job_a")]
        cursor.execute("SELECT set_config('juntai.tenant_id', 'tenant-b', false)")
        cursor.execute("SELECT tenant_id, job_id FROM juntai_synthetic_data.jobs ORDER BY job_id")
        assert cursor.fetchall() == [("tenant-b", "job_b")]
        cursor.execute("RESET ROLE")


@dataclass
class _Publisher:
    def publish(self, **_: Any) -> PublishedDataset:
        return PublishedDataset(
            artifact_id="art_acceptance",
            version_id="artv_acceptance_1",
            digest="sha256:" + "2" * 64,
            media_type="application/vnd.oci.image.manifest.v1+json",
        )


def _request() -> CreateJobRequest:
    return CreateJobRequest.model_validate(
        {
            "contract_version": "juntai.synthetic-data.request/v1",
            "generation_contract": {
                "contract_version": "juntai.synthetic-data.contract/v1",
                "records": [
                    {
                        "record_type": "record",
                        "count": {"maximum": 1},
                        "fields": {"id": {"type": "string", "unique": True}},
                    }
                ],
                "relations": [],
                "bounds": {"max_records": 1, "max_bytes": 4096, "max_shards": 1},
                "output": {"format": "jsonl", "compression": "none"},
            },
            "seed": "kes-acceptance",
            "provider": {"class": "tabular", "requirements": {"deterministic": True}},
            "policy": {"data_classification": "synthetic", "source_examples": "none"},
        }
    )


async def _api_worker_startup(dsn: str) -> None:
    repository = SqlJobRepository(lambda: psycopg.connect(dsn))
    service = SyntheticDataService(
        repository=repository,
        providers=ProviderRegistry(
            (DeterministicTabularProvider(worker_image_digest="sha256:" + "3" * 64),)
        ),
        policy=DefaultPolicyEngine(),
        quotas=InMemoryQuotaLedger(QuotaLimits()),
        publisher=_Publisher(),
        source_revision="a" * 40,
    )
    server = build_server(service, enable_runtime=False)
    assert server is not None
    created = service.create_job("tenant-startup", "startup-key", _request())
    scheduler = JobScheduler(service, poll_interval=0.01)
    await scheduler.validate()
    await scheduler.materialize()
    assert await scheduler.run_once() == 1
    assert service.get_job("tenant-startup", created.job_id).state.value == "SUCCEEDED"


def _primary(dsn: str, binding: MigrationBinding) -> dict[str, object]:
    _empty_repeat_and_check(dsn, binding)
    _concurrency(dsn, binding)
    _partial_failure(dsn, binding)
    _released_baseline_upgrade(dsn, binding)
    _reset(dsn)
    assert apply_migrations(dsn, binding).applied == ("0001_jobs",)
    _tenant_isolation(dsn)
    _reset(dsn)
    assert apply_migrations(dsn, binding).applied == ("0001_jobs",)
    asyncio.run(_api_worker_startup(dsn))
    return {
        "phase": "primary",
        "checks": [
            "empty-database",
            "repeat-idempotence",
            "concurrency-lock",
            "transactional-partial-failure",
            "released-1.0.0-baseline-upgrade",
            "tenant-rls-isolation",
            "post-migration-api-worker-startup",
        ],
    }


def _post_restart(dsn: str, binding: MigrationBinding) -> dict[str, object]:
    result = apply_migrations(dsn, binding)
    assert result.status == "current"
    assert result.current == ("0001_jobs",)
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
            "databaseVersion": _execute(dsn, "SELECT version()")[0][0],
        }
    )
    print(json.dumps(evidence, sort_keys=True))


if __name__ == "__main__":
    main()
