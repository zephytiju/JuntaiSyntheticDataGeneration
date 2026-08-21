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
    GenerationResult,
    GenerationState,
    ProviderView,
)
from juntai_synthetic_data.destinations import plan_destinations
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

from .models import CommitOutcome, GenerationWrite

T = TypeVar("T")

_TRANSIENT = (
    psycopg.OperationalError,
    psycopg.errors.SerializationFailure,
    psycopg.errors.DeadlockDetected,
)

_DESTINATION_INVALID = (
    psycopg.errors.CheckViolation,
    psycopg.errors.DatatypeMismatch,
    psycopg.errors.InvalidSchemaName,
    psycopg.errors.InvalidTextRepresentation,
    psycopg.errors.NotNullViolation,
    psycopg.errors.NumericValueOutOfRange,
    psycopg.errors.StringDataRightTruncation,
    psycopg.errors.UndefinedColumn,
    psycopg.errors.UndefinedTable,
)


class SqlGenerationRepository:
    def __init__(
        self,
        connector: Callable[[], AbstractContextManager[Any]],
        *,
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
            except _DESTINATION_INVALID as error:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_INVALID,
                    "KingbaseES rejected the caller-declared destination or generated values",
                ) from error
            except psycopg.errors.InsufficientPrivilege as error:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_FORBIDDEN,
                    "database tenant policy denied the destination write",
                ) from error
            except ValueError as error:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_INVALID,
                    "database driver rejected a caller-declared destination identifier",
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
            except (
                psycopg.errors.InvalidSchemaName,
                psycopg.errors.UndefinedTable,
                psycopg.errors.UndefinedColumn,
                ValueError,
            ) as error:
                raise SyntheticDataError(
                    ErrorCode.DELETE_CONFLICT,
                    "generated destination schema changed after the commit",
                ) from error

        return self._retry(operation)
