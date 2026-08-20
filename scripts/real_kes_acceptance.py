from __future__ import annotations

import argparse
import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg

from juntai_synthetic_data.api import build_server
from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.execution import WorkerCoordinator
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
from juntai_synthetic_data.service import SyntheticDataService
from juntai_synthetic_data.worker import SocketWorker, validate_worker_isolation
from juntai_synthetic_data.worker_protocol import (
    EVIDENCE_MEDIA_TYPE,
    INPUT_MEDIA_TYPE,
    PROTOCOL_VERSION,
    SOCKET_PATH,
    DispatchEnvelope,
    ExactArtifactReference,
    ProgressBounds,
    WorkerEventEnvelope,
    WorkloadIdentity,
    decode_envelope,
)

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
    assert first.applied == ("0001_jobs", "0002_worker_protocol")
    assert second.status == "current"
    assert checked.status == "current"


def _concurrency(dsn: str, binding: MigrationBinding) -> None:
    _reset(dsn)
    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _: apply_migrations(dsn, binding), range(4)))
    assert sum(result.applied == ("0001_jobs", "0002_worker_protocol") for result in results) == 1
    assert sum(result.status == "current" for result in results) == 3
    rows = _execute(
        dsn,
        "SELECT migration_id, count(*) FROM juntai_synthetic_data.schema_migrations "
        "GROUP BY migration_id ORDER BY migration_id",
    )
    assert rows == [("0001_jobs", 1), ("0002_worker_protocol", 1)], rows


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
    recovered = apply_migrations(dsn, binding)
    assert recovered.applied == ("0001_jobs", "0002_worker_protocol")


def _released_baseline_upgrade(dsn: str, binding: MigrationBinding) -> None:
    _reset(dsn)
    baseline = load_migrations()[0]
    released = MigrationBinding(
        source_revision="81c4b28336be46c57654d9de569ffefc803714f0",
        image_digest="sha256:" + "1" * 64,
        service_version="1.1.0",
    )
    assert apply_migrations(dsn, released, migrations=(baseline,)).applied == ("0001_jobs",)
    result = apply_migrations(dsn, binding)
    assert result.applied == ("0002_worker_protocol",)
    rows = _execute(
        dsn,
        "SELECT migration_id, service_version, adopted_from_baseline "
        "FROM juntai_synthetic_data.schema_migrations ORDER BY migration_id",
    )
    assert rows == [
        ("0001_jobs", "1.1.0", False),
        ("0002_worker_protocol", "1.2.0", False),
    ]


def _tenant_isolation(dsn: str) -> None:
    role = "synthetic_rls_acceptance"
    with psycopg.connect(dsn) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 1 FROM pg_roles WHERE rolname = %s", (role,))
        if cursor.fetchone() is None:
            cursor.execute(f"CREATE ROLE {role} NOLOGIN")
        cursor.execute(f"GRANT USAGE ON SCHEMA juntai_synthetic_data TO {role}")
        cursor.execute(f"GRANT SELECT ON ALL TABLES IN SCHEMA juntai_synthetic_data TO {role}")
        for tenant, job in (("tenant-a", "job_a"), ("tenant-b", "job_b")):
            suffix = tenant[-1]
            attempt = f"attempt-{suffix}"
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
            cursor.execute(
                """
                INSERT INTO juntai_synthetic_data.job_attempts (
                    tenant_id, job_id, attempt_id, attempt_number, input_artifact_json,
                    worker_image_digest, protocol_version, status
                ) VALUES (%s, %s, %s, 1, '{}'::jsonb, %s,
                          'juntai.synthetic.worker/v1', 'QUEUED')
                """,
                (tenant, job, attempt, "sha256:" + "2" * 64),
            )
            cursor.execute(
                """
                INSERT INTO juntai_synthetic_data.worker_outbox (
                    tenant_id, job_id, attempt_id, channel, message_id,
                    content_digest, canonical_bytes, sequence
                ) VALUES (%s, %s, %s, 'synthetic.worker.dispatch.v1', %s, %s, %s, 0)
                """,
                (tenant, job, attempt, f"message-{suffix}", "sha256:" + "3" * 64, b"{}"),
            )
            cursor.execute(
                """
                INSERT INTO juntai_synthetic_data.worker_result_inbox (
                    tenant_id, job_id, attempt_id, event_id, content_digest,
                    event_type, canonical_bytes, disposition
                ) VALUES (%s, %s, %s, %s, %s, 'STARTED', %s, 'COMMITTED')
                """,
                (tenant, job, attempt, f"event-{suffix}", "sha256:" + "4" * 64, b"{}"),
            )
            cursor.execute(
                """
                INSERT INTO juntai_synthetic_data.worker_cleanup_evidence (
                    tenant_id, job_id, attempt_id, evidence_id, reason_code, artifact_json
                ) VALUES (%s, %s, %s, %s, 'ATTEMPT_STALE', '{}'::jsonb)
                """,
                (tenant, job, attempt, f"cleanup-{suffix}"),
            )
        cursor.execute(f"SET ROLE {role}")
        cursor.execute("SELECT set_config('juntai.tenant_id', 'tenant-a', false)")
        cursor.execute("SELECT tenant_id, job_id FROM juntai_synthetic_data.jobs ORDER BY job_id")
        assert cursor.fetchall() == [("tenant-a", "job_a")]
        for table in (
            "job_attempts",
            "worker_outbox",
            "worker_result_inbox",
            "worker_cleanup_evidence",
        ):
            cursor.execute(f"SELECT tenant_id FROM juntai_synthetic_data.{table}")
            assert cursor.fetchall() == [("tenant-a",)]
        cursor.execute("SELECT set_config('juntai.tenant_id', 'tenant-b', false)")
        cursor.execute("SELECT tenant_id, job_id FROM juntai_synthetic_data.jobs ORDER BY job_id")
        assert cursor.fetchall() == [("tenant-b", "job_b")]
        for table in (
            "job_attempts",
            "worker_outbox",
            "worker_result_inbox",
            "worker_cleanup_evidence",
        ):
            cursor.execute(f"SELECT tenant_id FROM juntai_synthetic_data.{table}")
            assert cursor.fetchall() == [("tenant-b",)]
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


@dataclass
class _Inputs:
    def publish_input(self, job, *, source_revision: str) -> ExactArtifactReference:
        return ExactArtifactReference(
            tenantId=job.tenant_id,
            artifactId=f"art-input-{job.job_id}",
            versionId="artv-input-1",
            manifestDigest="sha256:" + "4" * 64,
            mediaType=INPUT_MEDIA_TYPE,
            byteLength=4096,
            producerBuildId=source_revision,
        )


class _WorkerEngine:
    def execute(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("socket worker must not execute during startup validation")

    def failure_evidence(self, *args: Any, **kwargs: Any) -> Any:
        raise AssertionError("socket worker must not publish during startup validation")


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


def _artifact(tenant_id: str, media_type: str, digit: str) -> ExactArtifactReference:
    return ExactArtifactReference(
        tenantId=tenant_id,
        artifactId=f"art-{digit}",
        versionId=f"artv-{digit}",
        manifestDigest="sha256:" + digit * 64,
        mediaType=media_type,
        byteLength=128,
        producerBuildId="a" * 40,
    )


def _worker_event(
    dispatch: DispatchEnvelope,
    *,
    event_id: str,
    event_type: str,
    stage: str,
    sequence: int,
    outcome: str | None = None,
) -> WorkerEventEnvelope:
    now = datetime.now(UTC)
    terminal = event_type == "TERMINAL"
    return WorkerEventEnvelope(
        messageId=f"message-{event_id}",
        tenantId=dispatch.tenant_id,
        jobId=dispatch.job_id,
        attemptId=dispatch.attempt_id,
        attemptNumber=dispatch.attempt_number,
        sequence=sequence,
        emittedAt=now,
        deadline=now + timedelta(minutes=5),
        correlationId=dispatch.correlation_id,
        producerWorkload=WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-executor"),
        eventId=event_id,
        eventType=event_type,
        executionLeaseId="lease-kes-acceptance",
        stage=stage,
        progressBounds=ProgressBounds(completed=1, total=1),
        observedCancelSequence=0,
        workerImageDigest=dispatch.worker_image_digest,
        protocolCapabilities=dispatch.required_capabilities,
        evidenceCounters={"events": sequence + 1, "shards": 1},
        outcome=outcome,
        datasetArtifact=_artifact(
            dispatch.tenant_id, "application/vnd.oci.image.manifest.v1+json", "6"
        )
        if outcome == "SUCCEEDED"
        else None,
        executionEvidenceArtifact=_artifact(dispatch.tenant_id, EVIDENCE_MEDIA_TYPE, "7")
        if terminal
        else None,
        startedAt=now if terminal else None,
        terminalAt=now if terminal else None,
        outputRecords=1 if outcome == "SUCCEEDED" else None,
        outputBytes=128 if outcome == "SUCCEEDED" else None,
        consumedInputDigest=dispatch.request_digest if terminal else None,
    ).signed()


def _api_worker_startup(dsn: str) -> None:
    repository = SqlJobRepository(lambda: psycopg.connect(dsn))
    api = WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-api")
    executor = WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-executor")
    coordinator = WorkerCoordinator(
        repository=repository,
        inputs=_Inputs(),
        source_revision="a" * 40,
        worker_image_digest="sha256:" + "3" * 64,
        api_workload=api,
        executor_workload=executor,
    )
    service = SyntheticDataService(
        repository=repository,
        providers=ProviderRegistry(
            (DeterministicTabularProvider(worker_image_digest="sha256:" + "3" * 64),)
        ),
        policy=DefaultPolicyEngine(),
        quotas=InMemoryQuotaLedger(QuotaLimits()),
        publisher=_Publisher(),
        source_revision="a" * 40,
        coordinator=coordinator,
    )
    server = build_server(service, enable_runtime=False)
    assert server is not None
    created = service.create_job("tenant-startup", "startup-key", _request())
    rows = _execute(
        dsn,
        "SELECT canonical_bytes FROM juntai_synthetic_data.worker_outbox "
        "WHERE tenant_id = %s AND job_id = %s",
        ("tenant-startup", created.job_id),
    )
    dispatch = decode_envelope(bytes(rows[0][0]))
    assert isinstance(dispatch, DispatchEnvelope)
    service.accept_worker_event(
        _worker_event(
            dispatch,
            event_id="event-kes-started",
            event_type="STARTED",
            stage="RUNNING",
            sequence=0,
        ),
        authenticated_producer=executor,
    )
    service.accept_worker_event(
        _worker_event(
            dispatch,
            event_id="event-kes-publishing",
            event_type="STAGE",
            stage="PUBLISHING",
            sequence=1,
        ),
        authenticated_producer=executor,
    )
    terminal = _worker_event(
        dispatch,
        event_id="event-kes-terminal",
        event_type="TERMINAL",
        stage="PUBLISHING",
        sequence=2,
        outcome="SUCCEEDED",
    )
    assert service.accept_worker_event(terminal, authenticated_producer=executor) == "COMMITTED"
    assert service.accept_worker_event(terminal, authenticated_producer=executor) == (
        "RESULT_DUPLICATE"
    )
    assert service.get_job("tenant-startup", created.job_id).state.value == "SUCCEEDED"
    assert len(service.result("tenant-startup", created.job_id).artifact.digest) == 71

    validate_worker_isolation(
        {
            "JUNTAI_SYNTHETIC_WORKER_PROTOCOL": PROTOCOL_VERSION,
            "JUNTAI_SYNTHETIC_WORKER_SOCKET": SOCKET_PATH,
        },
        mountinfo="tmpfs /var/run/juntai-worker",
    )
    worker = SocketWorker(
        _WorkerEngine(),
        workload=WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-worker"),
    )
    assert worker.engine.__class__ is _WorkerEngine


def _primary(dsn: str, binding: MigrationBinding) -> dict[str, object]:
    _empty_repeat_and_check(dsn, binding)
    _concurrency(dsn, binding)
    _partial_failure(dsn, binding)
    _released_baseline_upgrade(dsn, binding)
    _reset(dsn)
    assert apply_migrations(dsn, binding).applied == (
        "0001_jobs",
        "0002_worker_protocol",
    )
    _tenant_isolation(dsn)
    _reset(dsn)
    assert apply_migrations(dsn, binding).applied == (
        "0001_jobs",
        "0002_worker_protocol",
    )
    _api_worker_startup(dsn)
    return {
        "phase": "primary",
        "checks": [
            "empty-database",
            "repeat-idempotence",
            "concurrency-lock",
            "transactional-partial-failure",
            "transactional-failure-recovery",
            "released-1.1.0-baseline-upgrade",
            "tenant-rls-isolation-all-swp-tables",
            "atomic-outbox-result-replay",
            "post-migration-api-startup",
            "post-migration-worker-startup-no-kes",
        ],
    }


def _post_restart(dsn: str, binding: MigrationBinding) -> dict[str, object]:
    result = apply_migrations(dsn, binding)
    assert result.status == "current"
    assert result.current == ("0001_jobs", "0002_worker_protocol")
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
