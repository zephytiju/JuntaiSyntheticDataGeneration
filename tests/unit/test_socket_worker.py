from __future__ import annotations

import socket
import threading
import time
from datetime import UTC, datetime, timedelta

from juntai_synthetic_data.execution import WorkerExecutionResult
from juntai_synthetic_data.worker import SocketWorker
from juntai_synthetic_data.worker_protocol import (
    EVIDENCE_MEDIA_TYPE,
    INPUT_MEDIA_TYPE,
    REQUIRED_CAPABILITIES,
    CancelEnvelope,
    DispatchEnvelope,
    ExactArtifactReference,
    ResourceEnvelope,
    WorkerEventEnvelope,
    WorkloadIdentity,
    read_frame,
    write_frame,
)

API = WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-api")
EXECUTOR = WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-executor")


def artifact(media_type: str, digit: str) -> ExactArtifactReference:
    return ExactArtifactReference(
        tenantId="tenant-a",
        artifactId=f"art-{digit}",
        versionId=f"artv-{digit}",
        manifestDigest="sha256:" + digit * 64,
        mediaType=media_type,
        byteLength=128,
        producerBuildId="a" * 40,
    )


def dispatch() -> DispatchEnvelope:
    now = datetime.now(UTC)
    return DispatchEnvelope(
        messageId="dispatch-1",
        tenantId="tenant-a",
        jobId="job-1",
        attemptId="attempt-1",
        attemptNumber=1,
        sequence=0,
        emittedAt=now,
        deadline=now + timedelta(minutes=5),
        correlationId="job-1",
        producerWorkload=API,
        requestDigest="sha256:" + "1" * 64,
        inputArtifact=artifact(INPUT_MEDIA_TYPE, "2"),
        providerId="deterministic.tabular",
        providerVersion="1.0.0",
        workerImageDigest="sha256:" + "3" * 64,
        requiredCapabilities=REQUIRED_CAPABILITIES,
        minExecutorBinding="juntai.platform.synthetic-executor/v1",
        resourceEnvelope=ResourceEnvelope(
            cpuMillis=1000,
            memoryBytes=1_048_576,
            ephemeralBytes=1_048_576,
            processLimit=32,
        ),
        idempotencyKeyDigest="sha256:" + "4" * 64,
    ).signed()


class SuccessfulEngine:
    def execute(self, message, *, cancellation_requested, stage):
        assert not cancellation_requested()
        stage("RUNNING", 1, 1)
        now = datetime.now(UTC)
        return WorkerExecutionResult(
            dataset_artifact=artifact("application/vnd.oci.image.manifest.v1+json", "5"),
            evidence_artifact=artifact(EVIDENCE_MEDIA_TYPE, "6"),
            record_count=1,
            byte_count=128,
            started_at=now,
            terminal_at=now,
        )

    def failure_evidence(self, *args, **kwargs):
        raise AssertionError("success path must not publish failure evidence")


def test_socket_worker_emits_canonical_started_stage_and_terminal_frames() -> None:
    sidecar, worker_socket = socket.socketpair()
    worker = SocketWorker(SuccessfulEngine(), workload=EXECUTOR)  # type: ignore[arg-type]
    thread = threading.Thread(target=worker.process, args=(worker_socket, dispatch()))
    thread.start()
    events = [read_frame(sidecar) for _ in range(3)]
    thread.join(timeout=2)
    sidecar.close()
    worker_socket.close()
    assert not thread.is_alive()
    assert all(isinstance(event, WorkerEventEnvelope) for event in events)
    assert [event.event_type for event in events] == ["STARTED", "STAGE", "TERMINAL"]
    assert events[-1].outcome == "SUCCEEDED"
    assert events[-1].producer_workload == EXECUTOR


class CancellingEngine:
    def execute(self, message, *, cancellation_requested, stage):
        del message, stage
        deadline = time.monotonic() + 2
        while not cancellation_requested():
            if time.monotonic() >= deadline:
                raise AssertionError("worker did not observe cancellation")
            threading.Event().wait(0.01)
        raise InterruptedError("cancel observed")

    def failure_evidence(self, message, **kwargs):
        del message, kwargs
        return artifact(EVIDENCE_MEDIA_TYPE, "7")


def test_socket_worker_observes_monotonic_cancel_and_emits_cancelled_terminal() -> None:
    sidecar, worker_socket = socket.socketpair()
    engine = CancellingEngine()
    message = dispatch()
    worker = SocketWorker(engine, workload=EXECUTOR)  # type: ignore[arg-type]
    thread = threading.Thread(target=worker.process, args=(worker_socket, message))
    thread.start()
    started = read_frame(sidecar)
    assert isinstance(started, WorkerEventEnvelope)
    now = datetime.now(UTC)
    cancellation = CancelEnvelope(
        messageId="cancel-1",
        tenantId=message.tenant_id,
        jobId=message.job_id,
        attemptId=message.attempt_id,
        attemptNumber=message.attempt_number,
        sequence=1,
        emittedAt=now,
        deadline=now + timedelta(minutes=1),
        correlationId=message.correlation_id,
        producerWorkload=API,
        cancelSequence=1,
        requestedAt=now,
        reasonCode="caller-requested",
        requestedByKind="delegated",
        graceDeadline=now + timedelta(seconds=30),
    ).signed()
    write_frame(sidecar, cancellation)
    terminal = read_frame(sidecar)
    thread.join(timeout=2)
    sidecar.close()
    worker_socket.close()
    assert isinstance(terminal, WorkerEventEnvelope)
    assert terminal.outcome == "CANCELLED"
    assert terminal.observed_cancel_sequence == 1
