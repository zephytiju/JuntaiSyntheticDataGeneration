"""Transactional in-memory repository for deterministic service tests."""

from __future__ import annotations

import copy
import threading
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from juntai_synthetic_data.contracts.models import (
    DestinationResult,
    FieldType,
    GenerationResult,
    GenerationState,
    ProviderView,
    canonical_json,
)
from juntai_synthetic_data.destinations import plan_destinations
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

from .models import CommitOutcome, GenerationWrite


@dataclass(frozen=True)
class ForeignKeyDefinition:
    columns: tuple[str, ...]
    target_schema: str
    target_table: str
    target_columns: tuple[str, ...]


@dataclass(frozen=True)
class TableDefinition:
    columns: Mapping[str, FieldType]
    unique_keys: tuple[tuple[str, ...], ...]
    required_columns: frozenset[str] = frozenset()
    foreign_keys: tuple[ForeignKeyDefinition, ...] = ()


@dataclass(frozen=True)
class _WrittenRow:
    record_type: str
    schema: str
    table: str
    key: Mapping[str, Any]
    insert_ordinal: int
    delete_rank: int


class InMemoryGenerationRepository:
    def __init__(
        self,
        catalog: Mapping[tuple[str, str], TableDefinition],
        *,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.catalog = dict(catalog)
        self.clock = clock or (lambda: datetime.now(UTC))
        self._lock = threading.RLock()
        self._rows: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
        self._generations: dict[tuple[str, str], GenerationResult] = {}
        self._idempotency: dict[tuple[str, str], str] = {}
        self._request_digests: dict[tuple[str, str], str] = {}
        self._written: dict[tuple[str, str], tuple[_WrittenRow, ...]] = {}

    def seed_rows(
        self,
        tenant_id: str,
        schema: str,
        table: str,
        rows: tuple[Mapping[str, Any], ...],
    ) -> None:
        with self._lock:
            self._rows[(tenant_id, schema, table)] = [dict(row) for row in rows]

    def table_rows(self, tenant_id: str, schema: str, table: str) -> tuple[dict[str, Any], ...]:
        with self._lock:
            return tuple(copy.deepcopy(self._rows.get((tenant_id, schema, table), [])))

    def find_idempotent(self, tenant_id: str, idempotency_key: str) -> GenerationResult | None:
        with self._lock:
            generation_id = self._idempotency.get((tenant_id, idempotency_key))
            return self._generations.get((tenant_id, generation_id)) if generation_id else None

    def get(self, tenant_id: str, generation_id: str) -> GenerationResult | None:
        with self._lock:
            return self._generations.get((tenant_id, generation_id))

    @staticmethod
    def _conflicts(
        table: TableDefinition,
        existing: list[dict[str, Any]],
        row: dict[str, Any],
    ) -> bool:
        for key in table.unique_keys:
            candidate = tuple(row.get(column) for column in key)
            if any(value is None for value in candidate):
                continue
            if any(tuple(item.get(column) for column in key) == candidate for item in existing):
                return True
        return False

    def _enforce_database_write(
        self,
        *,
        tenant_id: str,
        record_type: str,
        table: TableDefinition,
        row: dict[str, Any],
        field_types: Mapping[str, FieldType],
        staged: Mapping[tuple[str, str, str], list[dict[str, Any]]],
    ) -> None:
        if not table.required_columns <= set(row):
            raise SyntheticDataError(
                ErrorCode.DESTINATION_INVALID,
                "KingbaseES rejected omitted required destination columns",
                details={"record_type": record_type},
            )
        for column, field_type in field_types.items():
            if table.columns.get(column) is not field_type:
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_INVALID,
                    "KingbaseES rejected a destination column or value type",
                    details={"record_type": record_type, "column": column},
                )
        for foreign_key in table.foreign_keys:
            candidate = tuple(row.get(column) for column in foreign_key.columns)
            if any(value is None for value in candidate):
                continue
            target_rows = staged.get(
                (tenant_id, foreign_key.target_schema, foreign_key.target_table), []
            )
            if not any(
                tuple(target.get(column) for column in foreign_key.target_columns) == candidate
                for target in target_rows
            ):
                raise SyntheticDataError(
                    ErrorCode.DESTINATION_CONFLICT,
                    "KingbaseES rejected a destination foreign-key value",
                    details={"record_type": record_type},
                )

    def commit(
        self,
        tenant_id: str,
        idempotency_key: str,
        write: GenerationWrite,
    ) -> CommitOutcome:
        with self._lock:
            existing = self.find_idempotent(tenant_id, idempotency_key)
            if existing is not None:
                prior = self._request_digests[(tenant_id, idempotency_key)]
                if prior != write.request.digest:
                    raise SyntheticDataError(
                        ErrorCode.IDEMPOTENCY_KEY_REUSED,
                        "Idempotency-Key was already used for different content",
                    )
                return CommitOutcome(existing, True)
            staged = copy.deepcopy(self._rows)
            contract = write.request.generation_contract
            specs = {record.record_type: record for record in contract.records}
            plan = plan_destinations(contract)
            delete_rank = {record_type: rank for rank, record_type in enumerate(plan.delete_order)}
            written: list[_WrittenRow] = []
            ordinal = 0
            for record_type in plan.insert_order:
                spec = specs[record_type]
                destination = spec.destination
                identity = (tenant_id, destination.schema_name, destination.table)
                table = self.catalog.get((destination.schema_name, destination.table))
                if table is None:
                    raise SyntheticDataError(
                        ErrorCode.DESTINATION_INVALID,
                        "KingbaseES rejected a nonexistent caller-declared destination",
                        details={
                            "schema": destination.schema_name,
                            "table": destination.table,
                        },
                    )
                rows = staged.setdefault(identity, [])
                for generated in write.dataset.records[record_type]:
                    database_row = {
                        column: generated[field] for field, column in destination.columns.items()
                    }
                    self._enforce_database_write(
                        tenant_id=tenant_id,
                        record_type=record_type,
                        table=table,
                        row=database_row,
                        field_types={
                            destination.columns[field]: spec.fields[field].type
                            for field in destination.columns
                        },
                        staged=staged,
                    )
                    if self._conflicts(table, rows, database_row):
                        raise SyntheticDataError(
                            ErrorCode.DESTINATION_CONFLICT,
                            "generated key conflicts with existing application data",
                            details={"record_type": record_type},
                        )
                    rows.append(database_row)
                    key = {
                        destination.columns[field]: generated[field]
                        for field in destination.key_fields
                    }
                    written.append(
                        _WrittenRow(
                            record_type,
                            destination.schema_name,
                            destination.table,
                            key,
                            ordinal,
                            delete_rank[record_type],
                        )
                    )
                    ordinal += 1
            created_at = self.clock()
            destinations = tuple(
                DestinationResult(
                    schema=record.destination.schema_name,
                    table=record.destination.table,
                    records_written=len(write.dataset.records[record.record_type]),
                )
                for record in contract.records
            )
            result = GenerationResult(
                generation_id=write.generation_id,
                state=GenerationState.COMMITTED,
                request_digest=write.request.digest,
                contract_digest=contract.digest,
                data_digest=write.dataset.data_digest,
                seed=write.request.seed,
                provider=ProviderView(
                    **{
                        "class": write.provider.provider_class,
                        "provider_id": write.provider.provider_id,
                        "version": write.provider.version,
                    }
                ),
                destinations=destinations,
                record_count=write.dataset.record_count,
                byte_count=write.dataset.byte_count,
                created_at=created_at,
            )
            self._rows = staged
            self._generations[(tenant_id, write.generation_id)] = result
            self._idempotency[(tenant_id, idempotency_key)] = write.generation_id
            self._request_digests[(tenant_id, idempotency_key)] = write.request.digest
            self._written[(tenant_id, write.generation_id)] = tuple(written)
            return CommitOutcome(result, False)

    def _has_reference(
        self,
        tenant_id: str,
        schema: str,
        table: str,
        row: Mapping[str, Any],
        staged: Mapping[tuple[str, str, str], list[dict[str, Any]]],
    ) -> bool:
        for (source_schema, source_table), definition in self.catalog.items():
            for foreign_key in definition.foreign_keys:
                if (foreign_key.target_schema, foreign_key.target_table) != (schema, table):
                    continue
                target = tuple(row.get(column) for column in foreign_key.target_columns)
                for candidate in staged.get((tenant_id, source_schema, source_table), []):
                    if tuple(candidate.get(column) for column in foreign_key.columns) == target:
                        return True
        return False

    def delete(self, tenant_id: str, generation_id: str) -> GenerationResult | None:
        with self._lock:
            result = self._generations.get((tenant_id, generation_id))
            if result is None:
                return None
            if result.state is GenerationState.DELETED:
                return result
            staged = copy.deepcopy(self._rows)
            ledger = sorted(
                self._written[(tenant_id, generation_id)],
                key=lambda item: (item.delete_rank, -item.insert_ordinal),
            )
            for written in ledger:
                identity = (tenant_id, written.schema, written.table)
                matches = [
                    index
                    for index, row in enumerate(staged.get(identity, []))
                    if all(row.get(column) == value for column, value in written.key.items())
                ]
                if len(matches) != 1:
                    raise SyntheticDataError(
                        ErrorCode.DELETE_CONFLICT,
                        "generated row no longer matches its exact written key",
                        details={"record_type": written.record_type},
                    )
                row = staged[identity][matches[0]]
                if self._has_reference(tenant_id, written.schema, written.table, row, staged):
                    raise SyntheticDataError(
                        ErrorCode.DELETE_CONFLICT,
                        "generated row has a later application reference",
                        details={"record_type": written.record_type},
                    )
                staged[identity].pop(matches[0])
            deleted = result.model_copy(
                update={"state": GenerationState.DELETED, "deleted_at": self.clock()}
            )
            self._rows = staged
            self._generations[(tenant_id, generation_id)] = deleted
            return deleted

    def ledger_bytes(self, tenant_id: str, generation_id: str) -> tuple[bytes, ...]:
        with self._lock:
            return tuple(
                canonical_json(item.key) for item in self._written[(tenant_id, generation_id)]
            )
