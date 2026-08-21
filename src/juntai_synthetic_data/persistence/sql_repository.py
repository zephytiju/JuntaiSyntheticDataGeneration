"""KingbaseES repository for atomic application rows and Synthetic metadata."""

from __future__ import annotations

import hashlib
import random
import time
from collections.abc import Callable
from contextlib import AbstractContextManager
from typing import Any, TypeVar

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb

from juntai_synthetic_data.contracts.models import (
    DestinationResult,
    FieldType,
    GenerationContract,
    GenerationResult,
    GenerationState,
    ProviderView,
)
from juntai_synthetic_data.destinations import DestinationAllowlist, plan_destinations
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

from .models import CommitOutcome, GenerationWrite

T = TypeVar("T")

_TRANSIENT = (
    psycopg.OperationalError,
    psycopg.errors.SerializationFailure,
    psycopg.errors.DeadlockDetected,
)

_DATABASE_TYPES: dict[FieldType, frozenset[str]] = {
    FieldType.STRING: frozenset(
        {"character varying", "character", "text", "name", "uuid", "citext"}
    ),
    FieldType.INTEGER: frozenset(
        {"smallint", "integer", "bigint", "numeric", "decimal", "int2", "int4", "int8"}
    ),
    FieldType.NUMBER: frozenset(
        {
            "smallint",
            "integer",
            "bigint",
            "numeric",
            "decimal",
            "real",
            "double precision",
            "float4",
            "float8",
        }
    ),
    FieldType.BOOLEAN: frozenset({"boolean", "bool"}),
    FieldType.DATE: frozenset({"date"}),
    FieldType.DATETIME: frozenset(
        {"timestamp without time zone", "timestamp with time zone", "timestamp", "timestamptz"}
    ),
}


class SqlGenerationRepository:
    def __init__(
        self,
        connector: Callable[[], AbstractContextManager[Any]],
        *,
        allowlist: DestinationAllowlist,
        retry_attempts: int = 3,
        retry_base_seconds: float = 0.05,
        retry_cap_seconds: float = 1.0,
        sleeper: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        if not 1 <= retry_attempts <= 10:
            raise ValueError("retry_attempts must be between 1 and 10")
        if retry_base_seconds < 0 or retry_cap_seconds < retry_base_seconds:
            raise ValueError("invalid retry timing")
        self.connector = connector
        self.allowlist = allowlist
        self.retry_attempts = retry_attempts
        self.retry_base_seconds = retry_base_seconds
        self.retry_cap_seconds = retry_cap_seconds
        self.sleeper = sleeper
        self.random_value = random_value

    @staticmethod
    def _tenant(cursor: Any, tenant_id: str) -> None:
        cursor.execute("SELECT set_config('juntai.tenant_id', %s, true)", (tenant_id,))

    def _retry(self, operation: Callable[[], T]) -> T:
        for attempt in range(self.retry_attempts):
            try:
                return operation()
            except _TRANSIENT as error:
                if attempt + 1 == self.retry_attempts:
                    raise SyntheticDataError(
                        ErrorCode.DEPENDENCY_UNAVAILABLE,
                        "application database remained unavailable after bounded retry",
                        retryable=True,
                    ) from error
                maximum = min(self.retry_cap_seconds, self.retry_base_seconds * (2**attempt))
                self.sleeper(maximum * self.random_value())
        raise AssertionError("unreachable retry loop")

    @staticmethod
    def _advisory_key(tenant_id: str, idempotency_key: str) -> int:
        digest = hashlib.sha256(f"{tenant_id}\0{idempotency_key}".encode()).digest()
        return int.from_bytes(digest[:8], "big", signed=True)

    @staticmethod
    def _result(row: tuple[Any, ...]) -> GenerationResult:
        destinations = tuple(DestinationResult.model_validate(item) for item in row[11])
        return GenerationResult(
            generation_id=str(row[0]),
            state=GenerationState(str(row[1])),
            request_digest=str(row[2]).strip(),
            contract_digest=str(row[3]).strip(),
            data_digest=str(row[4]).strip(),
            seed=str(row[5]),
            provider=ProviderView(
                **{
                    "class": str(row[6]),
                    "provider_id": str(row[7]),
                    "version": str(row[8]),
                }
            ),
            record_count=int(row[9]),
            byte_count=int(row[10]),
            destinations=destinations,
            created_at=row[12],
            deleted_at=row[13],
        )

    @staticmethod
    def _select_columns() -> str:
        return (
            "generation_id, state, request_digest, contract_digest, data_digest, seed, "
            "provider_class, provider_id, provider_version, record_count, byte_count, "
            "destinations_json, created_at, deleted_at"
        )

    def _find_idempotent_cursor(
        self, cursor: Any, tenant_id: str, idempotency_key: str
    ) -> GenerationResult | None:
        cursor.execute(
            f"SELECT {self._select_columns()} "
            "FROM juntai_synthetic_data.generations "
            "WHERE tenant_id = %s AND idempotency_key = %s",
            (tenant_id, idempotency_key),
        )
        row = cursor.fetchone()
        return self._result(row) if row else None

    def find_idempotent(self, tenant_id: str, idempotency_key: str) -> GenerationResult | None:
        def operation() -> GenerationResult | None:
            with (
                self.connector() as connection,
                connection.transaction(),
                connection.cursor() as cursor,
            ):
                self._tenant(cursor, tenant_id)
                return self._find_idempotent_cursor(cursor, tenant_id, idempotency_key)

        return self._retry(operation)

    def get(self, tenant_id: str, generation_id: str) -> GenerationResult | None:
        def operation() -> GenerationResult | None:
            with (
                self.connector() as connection,
                connection.transaction(),
                connection.cursor() as cursor,
            ):
                self._tenant(cursor, tenant_id)
                cursor.execute(
                    f"SELECT {self._select_columns()} "
                    "FROM juntai_synthetic_data.generations "
                    "WHERE tenant_id = %s AND generation_id = %s",
                    (tenant_id, generation_id),
                )
                row = cursor.fetchone()
                return self._result(row) if row else None

        return self._retry(operation)

    @staticmethod
    def _catalog_columns(cursor: Any, schema: str, table: str) -> dict[str, tuple[Any, ...]]:
        cursor.execute(
            """
            SELECT column_name, data_type, udt_name, is_nullable, column_default,
                   is_identity, is_generated
              FROM information_schema.columns
             WHERE table_schema = %s AND table_name = %s
             ORDER BY ordinal_position
            """,
            (schema, table),
        )
        return {str(row[0]): tuple(row[1:]) for row in cursor.fetchall()}

    @staticmethod
    def _unique_keys(cursor: Any, schema: str, table: str) -> set[frozenset[str]]:
        cursor.execute(
            """
            SELECT array_agg(attribute.attname ORDER BY position.ordinality)
              FROM pg_class AS relation
              JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
              JOIN pg_index AS index_def ON index_def.indrelid = relation.oid
              JOIN LATERAL unnest(index_def.indkey)
                   WITH ORDINALITY AS position(attnum, ordinality) ON true
              JOIN pg_attribute AS attribute
                ON attribute.attrelid = relation.oid
               AND attribute.attnum = position.attnum
             WHERE namespace.nspname = %s
               AND relation.relname = %s
               AND index_def.indisunique
               AND index_def.indpred IS NULL
             GROUP BY index_def.indexrelid
            """,
            (schema, table),
        )
        return {frozenset(str(item) for item in row[0]) for row in cursor.fetchall()}

    @staticmethod
    def _rls_applies_to_current_role(cursor: Any, schema: str, table: str) -> bool:
        cursor.execute(
            """
            SELECT relation.relrowsecurity,
                   relation.relforcerowsecurity,
                   pg_has_role(current_user, relation.relowner, 'MEMBER'),
                   current_role_definition.rolbypassrls
              FROM pg_class AS relation
              JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
              JOIN pg_roles AS current_role_definition
                ON current_role_definition.rolname = current_user
             WHERE namespace.nspname = %s AND relation.relname = %s
            """,
            (schema, table),
        )
        row = cursor.fetchone()
        if row is None:
            return False
        enabled, forced, role_owns_table, role_bypasses_rls = map(bool, row)
        return enabled and not role_bypasses_rls and (forced or not role_owns_table)

    @staticmethod
    def _foreign_key_exists(
        cursor: Any,
        *,
        source_schema: str,
        source_table: str,
        source_column: str,
        target_schema: str,
        target_table: str,
        target_column: str,
    ) -> bool:
        cursor.execute(
            """
            SELECT 1
              FROM pg_constraint AS constraint_def
              JOIN pg_class AS source_relation
                ON source_relation.oid = constraint_def.conrelid
              JOIN pg_namespace AS source_namespace
                ON source_namespace.oid = source_relation.relnamespace
              JOIN pg_class AS target_relation
                ON target_relation.oid = constraint_def.confrelid
              JOIN pg_namespace AS target_namespace
                ON target_namespace.oid = target_relation.relnamespace
              JOIN LATERAL generate_subscripts(constraint_def.conkey, 1)
                   AS position(index) ON true
              JOIN pg_attribute AS source_attribute
                ON source_attribute.attrelid = source_relation.oid
               AND source_attribute.attnum = constraint_def.conkey[position.index]
              JOIN pg_attribute AS target_attribute
                ON target_attribute.attrelid = target_relation.oid
               AND target_attribute.attnum = constraint_def.confkey[position.index]
             WHERE constraint_def.contype = 'f'
               AND source_namespace.nspname = %s
               AND source_relation.relname = %s
               AND source_attribute.attname = %s
               AND target_namespace.nspname = %s
               AND target_relation.relname = %s
               AND target_attribute.attname = %s
             LIMIT 1
            """,
            (
                source_schema,
                source_table,
                source_column,
                target_schema,
                target_table,
                target_column,
            ),
        )
        return cursor.fetchone() is not None

    @staticmethod
    def _compatible(field_type: FieldType, data_type: str, udt_name: str) -> bool:
        compatible_types = _DATABASE_TYPES[field_type]
        return data_type.lower() in compatible_types or udt_name.lower() in compatible_types

    def _validate_cursor(self, cursor: Any, contract: GenerationContract) -> None:
        records = {record.record_type: record for record in contract.records}
        for record in contract.records:
            destination = record.destination
            schema = destination.schema_name
            table_name = destination.table
            if not self.allowlist.allows(schema, table_name):
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_FORBIDDEN,
                    "destination is outside the deployment allowlist",
                    details={"schema": schema, "table": table_name},
                )
            columns = self._catalog_columns(cursor, schema, table_name)
            if not columns:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_INVALID,
                    "destination table does not exist",
                    details={"schema": schema, "table": table_name},
                )
            if not self._rls_applies_to_current_role(cursor, schema, table_name):
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_FORBIDDEN,
                    "destination table does not enforce tenant RLS for the service role",
                    details={"schema": schema, "table": table_name},
                )
            cursor.execute(
                "SELECT has_table_privilege(current_user, %s, 'INSERT,SELECT,DELETE')",
                (f'"{schema}"."{table_name}"',),
            )
            privilege = cursor.fetchone()
            if not privilege or not privilege[0]:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_FORBIDDEN,
                    "database role lacks required destination privileges",
                    details={"schema": schema, "table": table_name},
                )
            mapped_columns = set(destination.columns.values())
            for field_name, column in destination.columns.items():
                shape = columns.get(column)
                if shape is None or not self._compatible(
                    record.fields[field_name].type, str(shape[0]), str(shape[1])
                ):
                    raise SyntheticDataError(
                        ErrorCode.DESTINATION_INVALID,
                        "destination column type is incompatible",
                        details={"record_type": record.record_type, "column": column},
                    )
            required = {
                name
                for name, shape in columns.items()
                if str(shape[2]) == "NO"
                and shape[3] is None
                and str(shape[4]) != "YES"
                and str(shape[5]) in {"NEVER", "None", ""}
            }
            if not required <= mapped_columns:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_INVALID,
                    "destination omits a required column without a default",
                    details={"record_type": record.record_type},
                )
            physical_key = frozenset(destination.columns[field] for field in destination.key_fields)
            if physical_key not in self._unique_keys(cursor, schema, table_name):
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_INVALID,
                    "destination key_fields do not identify a unique database key",
                    details={"record_type": record.record_type},
                )
        for relation in contract.relations:
            source_type, source_field = relation.from_field.split(".", 1)
            target_type, target_field = relation.to_field.split(".", 1)
            source = records[source_type]
            target = records[target_type]
            if not self._foreign_key_exists(
                cursor,
                source_schema=source.destination.schema_name,
                source_table=source.destination.table,
                source_column=source.destination.columns[source_field],
                target_schema=target.destination.schema_name,
                target_table=target.destination.table,
                target_column=target.destination.columns[target_field],
            ):
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_INVALID,
                    "declared relation does not match a database foreign key",
                    details={"relation": f"{relation.from_field}->{relation.to_field}"},
                )

    def validate_destinations(self, tenant_id: str, contract: GenerationContract) -> None:
        def operation() -> None:
            with (
                self.connector() as connection,
                connection.transaction(),
                connection.cursor() as cursor,
            ):
                self._tenant(cursor, tenant_id)
                self._validate_cursor(cursor, contract)

        self._retry(operation)

    def commit(
        self,
        tenant_id: str,
        idempotency_key: str,
        write: GenerationWrite,
    ) -> CommitOutcome:
        def operation() -> CommitOutcome:
            try:
                with (
                    self.connector() as connection,
                    connection.transaction(),
                    connection.cursor() as cursor,
                ):
                    self._tenant(cursor, tenant_id)
                    cursor.execute(
                        "SELECT pg_advisory_xact_lock(%s)",
                        (self._advisory_key(tenant_id, idempotency_key),),
                    )
                    existing = self._find_idempotent_cursor(cursor, tenant_id, idempotency_key)
                    if existing is not None:
                        if existing.request_digest != write.request.digest:
                            raise SyntheticDataError(
                                ErrorCode.IDEMPOTENCY_KEY_REUSED,
                                "Idempotency-Key was already used for different content",
                            )
                        return CommitOutcome(existing, True)
                    contract = write.request.generation_contract
                    self._validate_cursor(cursor, contract)
                    specs = {record.record_type: record for record in contract.records}
                    plan = plan_destinations(contract)
                    delete_rank = {
                        record_type: rank for rank, record_type in enumerate(plan.delete_order)
                    }
                    ledger: list[tuple[Any, ...]] = []
                    ordinal = 0
                    for record_type in plan.insert_order:
                        spec = specs[record_type]
                        fields = tuple(sorted(spec.destination.columns))
                        columns = tuple(spec.destination.columns[field] for field in fields)
                        statement = sql.SQL("INSERT INTO {}.{} ({}) VALUES ({})").format(
                            sql.Identifier(spec.destination.schema_name),
                            sql.Identifier(spec.destination.table),
                            sql.SQL(", ").join(sql.Identifier(column) for column in columns),
                            sql.SQL(", ").join(sql.Placeholder() for _ in columns),
                        )
                        for record in write.dataset.records[record_type]:
                            cursor.execute(statement, tuple(record[field] for field in fields))
                            key = {
                                spec.destination.columns[field]: record[field]
                                for field in spec.destination.key_fields
                            }
                            ledger.append(
                                (
                                    tenant_id,
                                    write.generation_id,
                                    ordinal,
                                    record_type,
                                    spec.destination.schema_name,
                                    spec.destination.table,
                                    Jsonb(key),
                                    delete_rank[record_type],
                                )
                            )
                            ordinal += 1
                    destinations = [
                        DestinationResult(
                            schema=record.destination.schema_name,
                            table=record.destination.table,
                            records_written=len(write.dataset.records[record.record_type]),
                        ).model_dump(mode="json", by_alias=True)
                        for record in contract.records
                    ]
                    cursor.execute(
                        """
                        INSERT INTO juntai_synthetic_data.generations (
                            tenant_id, generation_id, idempotency_key, request_digest,
                            contract_digest, request_json, seed, provider_class, provider_id,
                            provider_version, policy_digest, state, data_digest, record_count,
                            byte_count, destinations_json
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, 'COMMITTED', %s, %s, %s, %s
                        )
                        """,
                        (
                            tenant_id,
                            write.generation_id,
                            idempotency_key,
                            write.request.digest,
                            contract.digest,
                            Jsonb(write.request.model_dump(mode="json", by_alias=True)),
                            write.request.seed,
                            write.provider.provider_class,
                            write.provider.provider_id,
                            write.provider.version,
                            write.policy_digest,
                            write.dataset.data_digest,
                            write.dataset.record_count,
                            write.dataset.byte_count,
                            Jsonb(destinations),
                        ),
                    )
                    if ledger:
                        cursor.executemany(
                            """
                            INSERT INTO juntai_synthetic_data.generation_rows (
                                tenant_id, generation_id, insert_ordinal, record_type,
                                destination_schema, destination_table, key_values, delete_rank
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            ledger,
                        )
                    result = self._find_idempotent_cursor(cursor, tenant_id, idempotency_key)
                    assert result is not None
                    return CommitOutcome(result, False)
            except SyntheticDataError:
                raise
            except (psycopg.errors.UniqueViolation, psycopg.errors.ForeignKeyViolation) as error:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_CONFLICT,
                    "generated data conflicts with the destination",
                ) from error
            except (psycopg.errors.CheckViolation, psycopg.errors.NotNullViolation) as error:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_INVALID,
                    "generated data violates a destination constraint",
                ) from error
            except psycopg.errors.InsufficientPrivilege as error:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_FORBIDDEN,
                    "database tenant policy denied the destination write",
                ) from error

        return self._retry(operation)

    def delete(self, tenant_id: str, generation_id: str) -> GenerationResult | None:
        def operation() -> GenerationResult | None:
            try:
                with (
                    self.connector() as connection,
                    connection.transaction(),
                    connection.cursor() as cursor,
                ):
                    self._tenant(cursor, tenant_id)
                    cursor.execute(
                        f"SELECT {self._select_columns()} "
                        "FROM juntai_synthetic_data.generations "
                        "WHERE tenant_id = %s AND generation_id = %s FOR UPDATE",
                        (tenant_id, generation_id),
                    )
                    row = cursor.fetchone()
                    if row is None:
                        return None
                    result = self._result(row)
                    if result.state is GenerationState.DELETED:
                        return result
                    cursor.execute(
                        """
                        SELECT destination_schema, destination_table, key_values
                          FROM juntai_synthetic_data.generation_rows
                         WHERE tenant_id = %s AND generation_id = %s
                         ORDER BY delete_rank, insert_ordinal DESC
                        """,
                        (tenant_id, generation_id),
                    )
                    for schema, table_name, raw_key in cursor.fetchall():
                        key = dict(raw_key)
                        columns = tuple(sorted(key))
                        statement = sql.SQL("DELETE FROM {}.{} WHERE {}").format(
                            sql.Identifier(str(schema)),
                            sql.Identifier(str(table_name)),
                            sql.SQL(" AND ").join(
                                sql.SQL("{} = {}").format(sql.Identifier(column), sql.Placeholder())
                                for column in columns
                            ),
                        )
                        cursor.execute(statement, tuple(key[column] for column in columns))
                        if cursor.rowcount != 1:
                            raise SyntheticDataError(
                                ErrorCode.DELETE_CONFLICT,
                                "generated row no longer matches its exact written key",
                            )
                    cursor.execute(
                        """
                        UPDATE juntai_synthetic_data.generations
                           SET state = 'DELETED', deleted_at = CURRENT_TIMESTAMP
                         WHERE tenant_id = %s AND generation_id = %s
                        """,
                        (tenant_id, generation_id),
                    )
                    cursor.execute(
                        f"SELECT {self._select_columns()} "
                        "FROM juntai_synthetic_data.generations "
                        "WHERE tenant_id = %s AND generation_id = %s",
                        (tenant_id, generation_id),
                    )
                    return self._result(cursor.fetchone())
            except SyntheticDataError:
                raise
            except psycopg.errors.ForeignKeyViolation as error:
                raise SyntheticDataError(
                    ErrorCode.DELETE_CONFLICT,
                    "generated row has a later application reference",
                ) from error
            except psycopg.errors.InsufficientPrivilege as error:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_FORBIDDEN,
                    "database tenant policy denied the destination deletion",
                ) from error
            except (psycopg.errors.UndefinedTable, psycopg.errors.UndefinedColumn) as error:
                raise SyntheticDataError(
                    ErrorCode.DELETE_CONFLICT,
                    "generated destination schema changed after the commit",
                ) from error

        return self._retry(operation)
