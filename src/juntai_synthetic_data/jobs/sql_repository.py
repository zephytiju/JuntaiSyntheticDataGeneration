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
            cancel_sequence,
            active_attempt_id,
            active_attempt_number,
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
            cancel_sequence=cancel_sequence,
            active_attempt_id=active_attempt_id,
            active_attempt_number=active_attempt_number,
        )

    @staticmethod
    def _select() -> str:
        return """
            SELECT job_id, tenant_id, idempotency_key, request_digest, request_json,
                   state, version, created_at, updated_at, quota_json, provider_id,
                   worker_image_digest, failure_json, result_json, cancellation_requested,
                   cancel_sequence, active_attempt_id, active_attempt_number
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
            self._save_cursor(cursor, job, expected_version=expected_version)
            self._insert_transitions(cursor, job)
        return job

    @staticmethod
    def _save_cursor(cursor: Any, job: Job, *, expected_version: int) -> None:
        cursor.execute(
            """
            UPDATE juntai_synthetic_data.jobs
               SET state = %s, version = %s, updated_at = %s, quota_json = %s::jsonb,
                   provider_id = %s, worker_image_digest = %s, failure_json = %s::jsonb,
                   result_json = %s::jsonb, cancellation_requested = %s,
                   cancel_sequence = %s, active_attempt_id = %s, active_attempt_number = %s
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
                job.cancel_sequence,
                job.active_attempt_id,
                job.active_attempt_number,
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

    def save_with_dispatch(
        self,
        job: Job,
        *,
        expected_version: int,
        input_artifact: Any,
        outbox: Any,
    ) -> Job:
        with (
            closing(self._connect()) as connection,
            connection.transaction(),
            connection.cursor() as cursor,
        ):
            self._save_cursor(cursor, job, expected_version=expected_version)
            self._insert_transitions(cursor, job)
            cursor.execute(
                """
                INSERT INTO juntai_synthetic_data.job_attempts (
                    tenant_id, job_id, attempt_id, attempt_number, input_artifact_json,
                    worker_image_digest, protocol_version, status
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s, 'QUEUED')
                """,
                (
                    job.tenant_id,
                    job.job_id,
                    outbox.attempt_id,
                    job.active_attempt_number,
                    json.dumps(input_artifact.model_dump(mode="json", by_alias=True)),
                    job.worker_image_digest,
                    "juntai.synthetic.worker/v1",
                ),
            )
            self._insert_outbox(cursor, outbox)
        return job

    def save_with_control(self, job: Job, *, expected_version: int, outbox: Any) -> Job:
        with (
            closing(self._connect()) as connection,
            connection.transaction(),
            connection.cursor() as cursor,
        ):
            self._save_cursor(cursor, job, expected_version=expected_version)
            self._insert_transitions(cursor, job)
            self._insert_outbox(cursor, outbox)
        return job

    @staticmethod
    def _insert_outbox(cursor: Any, outbox: Any) -> None:
        cursor.execute(
            """
            INSERT INTO juntai_synthetic_data.worker_outbox (
                tenant_id, job_id, attempt_id, channel, message_id,
                content_digest, canonical_bytes, sequence
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                outbox.tenant_id,
                outbox.job_id,
                outbox.attempt_id,
                outbox.channel,
                outbox.message_id,
                outbox.content_digest,
                outbox.canonical_bytes,
                outbox.sequence,
            ),
        )

    def worker_event_digest(self, event_id: str) -> str | None:
        with closing(self._connect()) as connection, connection.cursor() as cursor:
            cursor.execute(
                "SELECT content_digest FROM juntai_synthetic_data.worker_result_inbox "
                "WHERE event_id = %s",
                (event_id,),
            )
            row = cursor.fetchone()
            return str(row[0]).strip() if row else None

    def commit_worker_event(
        self, job: Job, *, expected_version: int, event: Any, disposition: str
    ) -> Job:
        with (
            closing(self._connect()) as connection,
            connection.transaction(),
            connection.cursor() as cursor,
        ):
            cursor.execute(
                """
                INSERT INTO juntai_synthetic_data.worker_result_inbox (
                    tenant_id, job_id, attempt_id, event_id, content_digest,
                    event_type, canonical_bytes, disposition
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                """,
                (
                    event.tenant_id,
                    event.job_id,
                    event.attempt_id,
                    event.event_id,
                    event.content_digest,
                    event.event_type,
                    event.canonical_bytes(),
                    disposition,
                ),
            )
            if cursor.rowcount != 1:
                cursor.execute(
                    "SELECT content_digest FROM juntai_synthetic_data.worker_result_inbox "
                    "WHERE event_id = %s",
                    (event.event_id,),
                )
                prior = str(cursor.fetchone()[0]).strip()
                if prior != event.content_digest:
                    raise SyntheticDataError(
                        ErrorCode.CONCURRENCY_CONFLICT, "worker event digest conflict"
                    )
                return job
            self._save_cursor(cursor, job, expected_version=expected_version)
            self._insert_transitions(cursor, job)
            cursor.execute(
                """
                UPDATE juntai_synthetic_data.job_attempts
                   SET status = %s, last_event_sequence = GREATEST(last_event_sequence, %s),
                       execution_lease_id = %s, updated_at = CURRENT_TIMESTAMP
                 WHERE tenant_id = %s AND job_id = %s AND attempt_id = %s
                """,
                (
                    event.outcome or event.stage,
                    event.sequence,
                    event.execution_lease_id,
                    event.tenant_id,
                    event.job_id,
                    event.attempt_id,
                ),
            )
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
