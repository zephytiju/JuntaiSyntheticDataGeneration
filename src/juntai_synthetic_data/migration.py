"""Ordered, transactional KingbaseES schema migrations owned by this service."""

from __future__ import annotations

import hashlib
import json
import os
import re
import stat
from collections.abc import Callable, Sequence
from contextlib import AbstractContextManager
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

import psycopg

from juntai_synthetic_data import __version__

DSN_FILE_ENV = "JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE"
SOURCE_REVISION_ENV = "JUNTAI_SOURCE_REVISION"
IMAGE_DIGEST_ENV = "JUNTAI_SERVICE_IMAGE_DIGEST"
MANIFEST_NAME = "manifest.v1.json"
_SOURCE_REVISION_PATTERN = re.compile(r"^[0-9a-f]{40}$")
_IMAGE_DIGEST_PATTERN = re.compile(r"^sha256:[0-9a-f]{64}$")


class MigrationError(RuntimeError):
    """Base error for the migration entry point."""


class MigrationConfigurationError(MigrationError):
    """Configuration is missing or cannot be used safely."""


class MigrationSafetyError(MigrationError):
    """The database state cannot be changed safely."""


class MigrationDatabaseError(MigrationError):
    """KingbaseES could not execute the migration transaction."""


@dataclass(frozen=True)
class Migration:
    migration_id: str
    path: str
    checksum: str
    sql: str
    baseline_version: str | None = None


@dataclass(frozen=True)
class MigrationBinding:
    source_revision: str
    image_digest: str
    service_version: str = __version__


@dataclass(frozen=True)
class MigrationResult:
    status: str
    applied: tuple[str, ...]
    adopted: tuple[str, ...]
    current: tuple[str, ...]
    pending: tuple[str, ...]

    def as_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "applied": list(self.applied),
            "adopted": list(self.adopted),
            "current": list(self.current),
            "pending": list(self.pending),
        }


def _migration_root() -> Any:
    packaged = resources.files("juntai_synthetic_data").joinpath("_migrations")
    if packaged.joinpath(MANIFEST_NAME).is_file():
        return packaged
    return Path(__file__).resolve().parents[2] / "migrations"


def manifest_text() -> str:
    return _migration_root().joinpath(MANIFEST_NAME).read_text(encoding="utf-8")


def manifest() -> dict[str, Any]:
    value = json.loads(manifest_text())
    if not isinstance(value, dict):
        raise MigrationSafetyError("migration manifest must be a JSON object")
    return value


def load_migrations() -> tuple[Migration, ...]:
    root = _migration_root()
    document = manifest()
    if document.get("service", {}).get("version") != __version__:
        raise MigrationSafetyError("migration manifest version differs from the installed service")
    migrations: list[Migration] = []
    seen: set[str] = set()
    for entry in document.get("migrations", []):
        migration_id = str(entry["id"])
        if migration_id in seen:
            raise MigrationSafetyError(f"duplicate migration ID: {migration_id}")
        seen.add(migration_id)
        path = str(entry["path"])
        if Path(path).name != path:
            raise MigrationSafetyError(f"migration path must be a local file name: {path}")
        sql = root.joinpath(path).read_text(encoding="utf-8")
        checksum = hashlib.sha256(sql.encode()).hexdigest()
        expected = str(entry["sha256"])
        if checksum != expected:
            raise MigrationSafetyError(
                f"migration checksum mismatch for {migration_id}: {checksum} != {expected}"
            )
        migrations.append(
            Migration(
                migration_id=migration_id,
                path=path,
                checksum=checksum,
                sql=sql,
                baseline_version=entry.get("releasedBaselineAdoption"),
            )
        )
    if not migrations:
        raise MigrationSafetyError("migration manifest is empty")
    if [item.migration_id for item in migrations] != sorted(
        item.migration_id for item in migrations
    ):
        raise MigrationSafetyError("migration manifest is not ordered by migration ID")
    return tuple(migrations)


def binding_from_environment() -> MigrationBinding:
    source_revision = os.getenv(SOURCE_REVISION_ENV, "")
    image_digest = os.getenv(IMAGE_DIGEST_ENV, "")
    if not _SOURCE_REVISION_PATTERN.fullmatch(source_revision):
        raise MigrationConfigurationError(
            f"{SOURCE_REVISION_ENV} must be a lowercase 40-character Git commit"
        )
    if not _IMAGE_DIGEST_PATTERN.fullmatch(image_digest):
        raise MigrationConfigurationError(
            f"{IMAGE_DIGEST_ENV} must be an immutable sha256 image digest"
        )
    return MigrationBinding(source_revision=source_revision, image_digest=image_digest)


def read_dsn_file(path_value: str | None = None) -> str:
    raw_path = path_value or os.getenv(DSN_FILE_ENV)
    if not raw_path:
        raise MigrationConfigurationError(f"{DSN_FILE_ENV} is required")
    path = Path(raw_path)
    if not path.is_absolute():
        raise MigrationConfigurationError(f"{DSN_FILE_ENV} must name an absolute path")
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as error:
        raise MigrationConfigurationError(f"cannot open KES DSN file: {path}") from error
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode):
            raise MigrationConfigurationError("KES DSN secret must be a regular file")
        if stat.S_IMODE(metadata.st_mode) & 0o077:
            raise MigrationConfigurationError("KES DSN secret must not be group/world accessible")
        if metadata.st_size > 8192:
            raise MigrationConfigurationError("KES DSN secret exceeds 8192 bytes")
        with os.fdopen(descriptor, encoding="utf-8") as stream:
            descriptor = -1
            dsn = stream.read().strip()
    finally:
        if descriptor >= 0:
            os.close(descriptor)
    if not dsn:
        raise MigrationConfigurationError("KES DSN secret is empty")
    return dsn


_LEDGER_SQL = """
CREATE TABLE IF NOT EXISTS juntai_synthetic_data.schema_migrations (
    migration_id           varchar(128) NOT NULL PRIMARY KEY,
    checksum               char(64) NOT NULL,
    service_version        varchar(32) NOT NULL,
    source_revision        char(40) NOT NULL,
    image_digest           char(71) NOT NULL,
    adopted_from_baseline  boolean NOT NULL DEFAULT false,
    applied_at             timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

_EXPECTED_COLUMNS: dict[str, dict[str, tuple[str, int | None, str]]] = {
    "jobs": {
        "job_id": ("character varying", 64, "NO"),
        "tenant_id": ("character varying", 128, "NO"),
        "idempotency_key": ("character varying", 200, "NO"),
        "request_digest": ("bpchar", 71, "NO"),
        "request_json": ("jsonb", None, "NO"),
        "state": ("character varying", 32, "NO"),
        "version": ("bigint", None, "NO"),
        "created_at": ("timestamp with time zone", None, "NO"),
        "updated_at": ("timestamp with time zone", None, "NO"),
        "quota_json": ("jsonb", None, "YES"),
        "provider_id": ("character varying", 128, "YES"),
        "worker_image_digest": ("bpchar", 71, "YES"),
        "failure_json": ("jsonb", None, "YES"),
        "result_json": ("jsonb", None, "YES"),
        "cancellation_requested": ("boolean", None, "NO"),
    },
    "job_transitions": {
        "tenant_id": ("character varying", 128, "NO"),
        "job_id": ("character varying", 64, "NO"),
        "sequence": ("integer", None, "NO"),
        "from_state": ("character varying", 32, "YES"),
        "to_state": ("character varying", 32, "NO"),
        "occurred_at": ("timestamp with time zone", None, "NO"),
        "reason": ("character varying", 500, "YES"),
    },
}


def _relation_exists(cursor: Any, relation: str) -> bool:
    cursor.execute("SELECT to_regclass(%s)", (relation,))
    return cursor.fetchone()[0] is not None


def _baseline_state(cursor: Any) -> str:
    relations = {
        name: _relation_exists(cursor, f"juntai_synthetic_data.{name}")
        for name in _EXPECTED_COLUMNS
    }
    if not any(relations.values()):
        return "absent"
    if not all(relations.values()):
        return "partial"
    cursor.execute(
        """
        SELECT table_name, column_name, data_type, character_maximum_length, is_nullable
          FROM information_schema.columns
         WHERE table_schema = 'juntai_synthetic_data'
           AND table_name IN ('jobs', 'job_transitions')
        """
    )
    actual_columns: dict[str, dict[str, tuple[str, int | None, str]]] = {
        name: {} for name in _EXPECTED_COLUMNS
    }
    for table_name, column_name, data_type, maximum_length, nullable in cursor.fetchall():
        actual_columns[table_name][column_name] = (data_type, maximum_length, nullable)
    for table_name, expected in _EXPECTED_COLUMNS.items():
        if any(
            actual_columns[table_name].get(column) != shape for column, shape in expected.items()
        ):
            return "partial"
    cursor.execute(
        """
        SELECT c.relname, c.relrowsecurity
          FROM pg_class AS c
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = 'juntai_synthetic_data'
           AND c.relname IN ('jobs', 'job_transitions')
        """
    )
    if {name for name, enabled in cursor.fetchall() if enabled} != set(_EXPECTED_COLUMNS):
        return "partial"
    cursor.execute(
        """
        SELECT tablename, policyname
          FROM pg_policies
         WHERE schemaname = 'juntai_synthetic_data'
        """
    )
    if not {
        ("jobs", "jobs_tenant_isolation"),
        ("job_transitions", "transitions_tenant_isolation"),
    }.issubset(set(cursor.fetchall())):
        return "partial"
    cursor.execute(
        """
        SELECT indexname, indexdef
          FROM pg_indexes
         WHERE schemaname = 'juntai_synthetic_data'
           AND indexname = 'jobs_runnable_idx'
        """
    )
    indexes = cursor.fetchall()
    if len(indexes) != 1:
        return "partial"
    index_definition = str(indexes[0][1]).lower()
    for required in ("jobs_runnable_idx", "state", "created_at", "job_id", "where"):
        if required not in index_definition:
            return "partial"
    return "complete"


_WORKER_TABLES = {
    "job_attempts",
    "worker_outbox",
    "worker_result_inbox",
    "worker_cleanup_evidence",
}
_WORKER_POLICIES = {
    ("job_attempts", "attempts_tenant_isolation"),
    ("worker_outbox", "worker_outbox_tenant_isolation"),
    ("worker_result_inbox", "worker_inbox_tenant_isolation"),
    ("worker_cleanup_evidence", "cleanup_evidence_tenant_isolation"),
}


def _worker_protocol_state(cursor: Any) -> str:
    existing = {
        table
        for table in _WORKER_TABLES
        if _relation_exists(cursor, f"juntai_synthetic_data.{table}")
    }
    if not existing:
        return "absent"
    if existing != _WORKER_TABLES:
        return "partial"
    cursor.execute(
        """
        SELECT c.relname, c.relrowsecurity
          FROM pg_class AS c
          JOIN pg_namespace AS n ON n.oid = c.relnamespace
         WHERE n.nspname = 'juntai_synthetic_data'
           AND c.relname IN (
               'job_attempts', 'worker_outbox',
               'worker_result_inbox', 'worker_cleanup_evidence'
           )
        """
    )
    if {name for name, enabled in cursor.fetchall() if enabled} != _WORKER_TABLES:
        return "partial"
    cursor.execute(
        "SELECT tablename, policyname FROM pg_policies WHERE schemaname = 'juntai_synthetic_data'"
    )
    if not _WORKER_POLICIES.issubset(set(cursor.fetchall())):
        return "partial"
    cursor.execute(
        """
        SELECT column_name
          FROM information_schema.columns
         WHERE table_schema = 'juntai_synthetic_data'
           AND table_name = 'jobs'
           AND column_name IN ('cancel_sequence', 'active_attempt_id', 'active_attempt_number')
        """
    )
    if {row[0] for row in cursor.fetchall()} != {
        "cancel_sequence",
        "active_attempt_id",
        "active_attempt_number",
    }:
        return "partial"
    return "complete"


def _verify_server(cursor: Any, required_prefix: str) -> None:
    cursor.execute("SELECT version()")
    version = str(cursor.fetchone()[0])
    if not version.startswith(required_prefix):
        raise MigrationSafetyError(
            f"unsupported database product/version; expected prefix {required_prefix!r}"
        )


def _read_ledger(cursor: Any) -> dict[str, str]:
    if not _relation_exists(cursor, "juntai_synthetic_data.schema_migrations"):
        return {}
    cursor.execute(
        "SELECT migration_id, checksum FROM juntai_synthetic_data.schema_migrations "
        "ORDER BY migration_id"
    )
    return {
        str(migration_id): str(checksum).strip() for migration_id, checksum in cursor.fetchall()
    }


def _record(cursor: Any, migration: Migration, binding: MigrationBinding, adopted: bool) -> None:
    cursor.execute(
        """
        INSERT INTO juntai_synthetic_data.schema_migrations (
            migration_id, checksum, service_version, source_revision,
            image_digest, adopted_from_baseline
        ) VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            migration.migration_id,
            migration.checksum,
            binding.service_version,
            binding.source_revision,
            binding.image_digest,
            adopted,
        ),
    )


def apply_migrations(
    dsn: str,
    binding: MigrationBinding,
    *,
    check: bool = False,
    migrations: Sequence[Migration] | None = None,
    connector: Callable[[str], AbstractContextManager[Any]] = psycopg.connect,
) -> MigrationResult:
    ordered = tuple(migrations or load_migrations())
    expected = {migration.migration_id: migration for migration in ordered}
    manifest_document = manifest()
    required_prefix = str(manifest_document["database"]["requiredVersionPrefix"])
    lock_id = int(manifest_document["database"]["transactionLockId"])
    try:
        with (
            connector(dsn) as connection,
            connection.transaction(),
            connection.cursor() as cursor,
        ):
            _verify_server(cursor, required_prefix)
            cursor.execute("SELECT pg_advisory_xact_lock(%s)", (lock_id,))
            baseline_state = _baseline_state(cursor)
            ledger = _read_ledger(cursor)
            unknown = sorted(set(ledger) - set(expected))
            if unknown:
                raise MigrationSafetyError(
                    "database contains migrations unknown to this image: " + ", ".join(unknown)
                )
            for migration_id, checksum in ledger.items():
                if checksum != expected[migration_id].checksum:
                    raise MigrationSafetyError(f"applied migration checksum drift: {migration_id}")
            if ledger and baseline_state != "complete":
                raise MigrationSafetyError(
                    "migration ledger exists but baseline schema is incomplete"
                )
            pending = tuple(item for item in ordered if item.migration_id not in ledger)
            if check:
                if not pending and _worker_protocol_state(cursor) != "complete":
                    raise MigrationSafetyError("SWP/v1 schema is incomplete")
                status = "pending" if pending else "current"
                return MigrationResult(
                    status=status,
                    applied=(),
                    adopted=(),
                    current=tuple(ledger),
                    pending=tuple(item.migration_id for item in pending),
                )
            if baseline_state == "partial":
                raise MigrationSafetyError(
                    "partial or incompatible released baseline detected; "
                    "refusing repair or downgrade"
                )
            applied: list[str] = []
            adopted: list[str] = []
            for index, migration in enumerate(pending):
                is_baseline = index == 0 and baseline_state == "complete" and not ledger
                if is_baseline:
                    if migration.baseline_version is None:
                        raise MigrationSafetyError(
                            "complete untracked schema has no authorized baseline adoption"
                        )
                    cursor.execute(_LEDGER_SQL)
                    _record(cursor, migration, binding, True)
                    adopted.append(migration.migration_id)
                else:
                    cursor.execute(migration.sql)
                    cursor.execute(_LEDGER_SQL)
                    _record(cursor, migration, binding, False)
                    applied.append(migration.migration_id)
                ledger[migration.migration_id] = migration.checksum
                baseline_state = "complete"
            if _baseline_state(cursor) != "complete":
                raise MigrationSafetyError("post-migration schema verification failed")
            if any(item.migration_id == "0002_worker_protocol" for item in ordered) and (
                _worker_protocol_state(cursor) != "complete"
            ):
                raise MigrationSafetyError("post-migration SWP/v1 schema verification failed")
            return MigrationResult(
                status="applied" if applied or adopted else "current",
                applied=tuple(applied),
                adopted=tuple(adopted),
                current=tuple(ledger),
                pending=(),
            )
    except MigrationSafetyError:
        raise
    except psycopg.Error as error:
        raise MigrationDatabaseError("KingbaseES migration transaction failed") from error
