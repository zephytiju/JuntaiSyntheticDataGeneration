"""KingbaseES/PostgreSQL-compatible bounded job metadata repository."""

from __future__ import annotations

import json
from collections.abc import Callable
from contextlib import closing
from typing import Any

from juntai_synthetic_data.contracts.models import CreateJobRequest
from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError

from .models import Job, JobState, Transition


class SqlJobRepository:
    """Persists only bounded metadata; dataset rows and bytes never enter KES."""

    def __init__(self, connect: Callable[[], Any]) -> None:
        self._connect = connect

    @staticmethod
    def _job(row: tuple[Any, ...], transitions: list[Transition]) -> Job:
        (
            job_id,
            tenant_id,
            idempotency_key,
            request_digest,
            request,
            state,
            version,
            created_at,
            updated_at,
            quota,
            provider_id,
            worker_image_digest,
            failure,
            result,
            cancellation_requested,
        ) = row
        return Job(
            job_id=job_id,
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            request_digest=request_digest,
            request=CreateJobRequest.model_validate(request),
            state=JobState(state),
            version=version,
            created_at=created_at,
            updated_at=updated_at,
            transitions=transitions,
            quota=quota,
            provider_id=provider_id,
            worker_image_digest=worker_image_digest,
            failure=failure,
            result=result,
            cancellation_requested=cancellation_requested,
        )

    @staticmethod
    def _select() -> str:
        return """
            SELECT job_id, tenant_id, idempotency_key, request_digest, request_json,
                   state, version, created_at, updated_at, quota_json, provider_id,
                   worker_image_digest, failure_json, result_json, cancellation_requested
              FROM juntai_synthetic_data.jobs
        """

    @staticmethod
    def _transition_rows(cursor: Any, tenant_id: str, job_id: str) -> list[Transition]:
        cursor.execute(
            """
            SELECT sequence, from_state, to_state, occurred_at, reason
              FROM juntai_synthetic_data.job_transitions
             WHERE tenant_id = %s AND job_id = %s
             ORDER BY sequence
            """,
            (tenant_id, job_id),
        )
        return [
            Transition(
                sequence=row[0],
                from_state=JobState(row[1]) if row[1] else None,
                to_state=JobState(row[2]),
                occurred_at=row[3],
                reason=row[4],
            )
            for row in cursor.fetchall()
        ]

    def create(self, job: Job) -> Job:
        try:
            with (
                closing(self._connect()) as connection,
                connection.transaction(),
                connection.cursor() as cursor,
            ):
                cursor.execute(
                    """
                    INSERT INTO juntai_synthetic_data.jobs
                        (job_id, tenant_id, idempotency_key, request_digest, request_json,
                         state, version, created_at, updated_at, quota_json, provider_id,
                         worker_image_digest, failure_json, result_json, cancellation_requested)
                    VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s::jsonb,
                            %s, %s, %s::jsonb, %s::jsonb, %s)
                    """,
                    (
                        job.job_id,
                        job.tenant_id,
                        job.idempotency_key,
                        job.request_digest,
                        job.request.model_dump_json(exclude_none=True, by_alias=True),
                        job.state.value,
                        job.version,
                        job.created_at,
                        job.updated_at,
                        json.dumps(job.quota),
                        job.provider_id,
                        job.worker_image_digest,
                        json.dumps(job.failure),
                        json.dumps(job.result, default=str),
                        job.cancellation_requested,
                    ),
                )
                self._insert_transitions(cursor, job)
        except Exception as exc:
            if "unique" in str(exc).lower() or "duplicate" in str(exc).lower():
                raise SyntheticDataError(
                    ErrorCode.CONCURRENCY_CONFLICT, "job already exists"
                ) from exc
            raise
        return job

    def get(self, tenant_id: str, job_id: str) -> Job | None:
        with closing(self._connect()) as connection, connection.cursor() as cursor:
            cursor.execute(
                self._select() + " WHERE tenant_id = %s AND job_id = %s", (tenant_id, job_id)
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self._job(row, self._transition_rows(cursor, tenant_id, job_id))

    def find_idempotent(self, tenant_id: str, idempotency_key: str) -> Job | None:
        with closing(self._connect()) as connection, connection.cursor() as cursor:
            cursor.execute(
                self._select() + " WHERE tenant_id = %s AND idempotency_key = %s",
                (tenant_id, idempotency_key),
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return self._job(row, self._transition_rows(cursor, tenant_id, row[0]))

    def save(self, job: Job, *, expected_version: int) -> Job:
        with (
            closing(self._connect()) as connection,
            connection.transaction(),
            connection.cursor() as cursor,
        ):
            cursor.execute(
                """
                UPDATE juntai_synthetic_data.jobs
                   SET state = %s, version = %s, updated_at = %s, quota_json = %s::jsonb,
                       provider_id = %s, worker_image_digest = %s, failure_json = %s::jsonb,
                       result_json = %s::jsonb, cancellation_requested = %s
                 WHERE tenant_id = %s AND job_id = %s AND version = %s
                """,
                (
                    job.state.value,
                    job.version,
                    job.updated_at,
                    json.dumps(job.quota),
                    job.provider_id,
                    job.worker_image_digest,
                    json.dumps(job.failure),
                    json.dumps(job.result, default=str),
                    job.cancellation_requested,
                    job.tenant_id,
                    job.job_id,
                    expected_version,
                ),
            )
            if cursor.rowcount != 1:
                raise SyntheticDataError(
                    ErrorCode.CONCURRENCY_CONFLICT,
                    "job version changed during update",
                    retryable=True,
                )
            self._insert_transitions(cursor, job)
        return job

    def list_runnable(self, *, limit: int = 100) -> tuple[Job, ...]:
        with closing(self._connect()) as connection, connection.cursor() as cursor:
            cursor.execute(
                self._select() + " WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELLED')"
                " ORDER BY created_at, job_id LIMIT %s",
                (limit,),
            )
            rows = cursor.fetchall()
            return tuple(
                self._job(row, self._transition_rows(cursor, row[1], row[0])) for row in rows
            )

    @staticmethod
    def _insert_transitions(cursor: Any, job: Job) -> None:
        for transition in job.transitions:
            cursor.execute(
                """
                INSERT INTO juntai_synthetic_data.job_transitions
                    (tenant_id, job_id, sequence, from_state, to_state, occurred_at, reason)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (tenant_id, job_id, sequence) DO NOTHING
                """,
                (
                    job.tenant_id,
                    job.job_id,
                    transition.sequence,
                    transition.from_state.value if transition.from_state else None,
                    transition.to_state.value,
                    transition.occurred_at,
                    transition.reason,
                ),
            )
