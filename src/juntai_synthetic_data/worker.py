"""Isolated SWP/v1 worker process with no database, queue, API, or Kubernetes client."""

from __future__ import annotations

import os
import socket
import threading
import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path

from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.execution import WorkerEngine
from juntai_synthetic_data.worker_protocol import (
    PROTOCOL_VERSION,
    SOCKET_PATH,
    CancelEnvelope,
    DispatchEnvelope,
    ProtocolError,
    StableWorkerError,
    WorkerEventEnvelope,
    WorkloadIdentity,
    read_frame,
    write_frame,
)

PROTOCOL_ENV = "JUNTAI_SYNTHETIC_WORKER_PROTOCOL"
SOCKET_ENV = "JUNTAI_SYNTHETIC_WORKER_SOCKET"
_FORBIDDEN_EXACT = frozenset(
    {
        "JUNTAI_JOB_DATABASE_DSN",
        "JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE",
        "KUBERNETES_API_SERVER",
        "KUBERNETES_TOKEN_REVIEW_TOKEN_FILE",
        "KUBERNETES_CA_FILE",
        "KUBECONFIG",
    }
)
_FORBIDDEN_MARKERS = ("KES", "QUEUE", "BROKER", "SYNTHETIC_API", "TOKEN_REVIEW")
_FORBIDDEN_MOUNT_MARKERS = (
    "serviceaccount/token",
    "token-reviewer",
    "kubeconfig",
    "kes-dsn",
    "kingbase",
    "queue-token",
)


def validate_worker_isolation(
    environ: Mapping[str, str] | None = None,
    *,
    mountinfo: str | None = None,
) -> None:
    values = dict(os.environ if environ is None else environ)
    if values.get(PROTOCOL_ENV) != PROTOCOL_VERSION:
        raise RuntimeError(f"{PROTOCOL_ENV} must be {PROTOCOL_VERSION}")
    if values.get(SOCKET_ENV, SOCKET_PATH) != SOCKET_PATH:
        raise RuntimeError(f"{SOCKET_ENV} must be {SOCKET_PATH}")
    forbidden = sorted(
        name
        for name in values
        if name in _FORBIDDEN_EXACT
        or name.startswith("KUBERNETES_")
        or any(marker in name.upper() for marker in _FORBIDDEN_MARKERS)
    )
    if forbidden:
        raise RuntimeError("forbidden worker configuration present: " + ", ".join(forbidden))
    if mountinfo is None:
        path = Path("/proc/self/mountinfo")
        mountinfo = path.read_text(errors="replace") if path.exists() else ""
    lowered = mountinfo.lower()
    mounted = [marker for marker in _FORBIDDEN_MOUNT_MARKERS if marker in lowered]
    if mounted:
        raise RuntimeError("forbidden worker credential mount present: " + ", ".join(mounted))


class _Cancellation:
    def __init__(self) -> None:
        self.sequence = 0
        self.event = threading.Event()

    def observe(self, message: CancelEnvelope) -> None:
        if message.cancel_sequence > self.sequence:
            self.sequence = message.cancel_sequence
            self.event.set()


class SocketWorker:
    def __init__(self, engine: WorkerEngine, *, workload: WorkloadIdentity) -> None:
        self.engine = engine
        self.workload = workload

    @staticmethod
    def _now() -> datetime:
        return datetime.now(UTC)

    def run(self, *, socket_path: str = SOCKET_PATH) -> None:
        validate_worker_isolation()
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            stream.connect(socket_path)
            first = read_frame(stream)
            if not isinstance(first, DispatchEnvelope):
                raise ProtocolError("ENVELOPE_INVALID", "first SWP frame must be dispatch")
            self.process(stream, first)

    def process(self, stream: socket.socket, dispatch: DispatchEnvelope) -> None:
        dispatch.verify()
        cancellation = _Cancellation()
        sequence = 0
        started_at = self._now()
        write_frame(stream, self._event(dispatch, "STARTED", "RUNNING", sequence).signed())
        sequence += 1

        def listen() -> None:
            while True:
                try:
                    message = read_frame(stream)
                except (OSError, ProtocolError):
                    return
                if not isinstance(message, CancelEnvelope):
                    return
                if (
                    message.tenant_id != dispatch.tenant_id
                    or message.job_id != dispatch.job_id
                    or message.attempt_id != dispatch.attempt_id
                ):
                    return
                cancellation.observe(message)

        listener = threading.Thread(target=listen, name="swp-cancellation", daemon=True)
        listener.start()

        def stage(name: str, completed: int, total: int) -> None:
            nonlocal sequence
            write_frame(
                stream,
                self._event(
                    dispatch,
                    "STAGE",
                    name,
                    sequence,
                    observed_cancel=cancellation.sequence,
                    completed=completed,
                    total=total,
                ).signed(),
            )
            sequence += 1

        try:
            if self._now() >= dispatch.deadline:
                raise TimeoutError("dispatch deadline already expired")
            result = self.engine.execute(
                dispatch,
                cancellation_requested=cancellation.event.is_set,
                stage=stage,
            )
            terminal = self._event(
                dispatch,
                "TERMINAL",
                "PUBLISHING",
                sequence,
                observed_cancel=cancellation.sequence,
                outcome="SUCCEEDED",
                dataset=result.dataset_artifact,
                evidence=result.evidence_artifact,
                started_at=result.started_at,
                terminal_at=result.terminal_at,
                records=result.record_count,
                byte_count=result.byte_count,
            )
        except Exception as error:
            terminal_at = self._now()
            if isinstance(error, InterruptedError):
                outcome, code, retryable = "CANCELLED", "CANCELLED", False
            elif isinstance(error, TimeoutError):
                outcome, code, retryable = "DEADLINE_EXCEEDED", "DEADLINE_EXCEEDED", False
            elif isinstance(error, SyntheticDataError):
                outcome = "FAILED"
                code = {
                    ErrorCode.PUBLICATION_FAILED: "PUBLICATION_FAILED",
                    ErrorCode.DEPENDENCY_UNAVAILABLE: "DEPENDENCY_UNAVAILABLE",
                    ErrorCode.CONTRACT_INVALID: "ARTIFACT_INTEGRITY_FAILED",
                }.get(error.code, "DEPENDENCY_UNAVAILABLE")
                retryable = error.retryable
            else:
                outcome, code, retryable = "FAILED", "DEPENDENCY_UNAVAILABLE", True
            evidence = self.engine.failure_evidence(
                dispatch,
                code=code,
                started_at=started_at,
                terminal_at=terminal_at,
            )
            terminal = self._event(
                dispatch,
                "TERMINAL",
                "RUNNING",
                sequence,
                observed_cancel=cancellation.sequence,
                outcome=outcome,
                evidence=evidence,
                started_at=started_at,
                terminal_at=terminal_at,
                error=StableWorkerError(code=code, retryable=retryable, message=str(error)[:500]),
            )
        write_frame(stream, terminal.signed())

    def _event(
        self,
        dispatch: DispatchEnvelope,
        event_type: str,
        stage: str,
        sequence: int,
        *,
        observed_cancel: int = 0,
        completed: int = 0,
        total: int = 0,
        outcome: str | None = None,
        dataset=None,
        evidence=None,
        started_at: datetime | None = None,
        terminal_at: datetime | None = None,
        records: int | None = None,
        byte_count: int | None = None,
        error: StableWorkerError | None = None,
    ) -> WorkerEventEnvelope:
        event_id = f"evt_{uuid.uuid4().hex}"
        return WorkerEventEnvelope.model_validate(
            {
                "protocolVersion": PROTOCOL_VERSION,
                "kind": "worker-event",
                "messageId": f"msg_{uuid.uuid4().hex}",
                "tenantId": dispatch.tenant_id,
                "jobId": dispatch.job_id,
                "attemptId": dispatch.attempt_id,
                "attemptNumber": dispatch.attempt_number,
                "sequence": sequence,
                "emittedAt": self._now(),
                "deadline": dispatch.deadline,
                "correlationId": dispatch.correlation_id,
                "traceparent": dispatch.traceparent,
                "producerWorkload": self.workload.model_dump(by_alias=True),
                "eventId": event_id,
                "eventType": event_type,
                "executionLeaseId": f"lease_{dispatch.attempt_id}",
                "stage": stage,
                "progressBounds": {"completed": completed, "total": total},
                "observedCancelSequence": observed_cancel,
                "workerImageDigest": dispatch.worker_image_digest,
                "protocolCapabilities": dispatch.required_capabilities,
                "evidenceCounters": {"events": sequence + 1},
                "outcome": outcome,
                "error": error.model_dump(by_alias=True) if error else None,
                "datasetArtifact": dataset.model_dump(by_alias=True) if dataset else None,
                "executionEvidenceArtifact": evidence.model_dump(by_alias=True)
                if evidence
                else None,
                "startedAt": started_at,
                "terminalAt": terminal_at,
                "outputRecords": records,
                "outputBytes": byte_count,
                "consumedInputDigest": dispatch.request_digest if outcome is not None else None,
            }
        )
