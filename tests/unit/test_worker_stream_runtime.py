from __future__ import annotations

import hashlib
import time
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pytest
from juntai_platform_swp_stream import (
    Accepted,
    StreamFrame,
    WorkerStreamAuthenticationError,
    WorkerStreamAuthorizationError,
    WorkerStreamClaimError,
    WorkerStreamClosedError,
    WorkerStreamConfigurationError,
    WorkerStreamConflictError,
    WorkerStreamFrameError,
    WorkerStreamKeepaliveError,
    WorkerStreamProtocolError,
    WorkerStreamSequenceError,
    WorkerStreamTimeoutError,
    WorkerStreamUnavailableError,
)
from test_socket_worker import EXECUTOR, SuccessfulEngine, dispatch

from juntai_synthetic_data.platform_adapter_contract import PLATFORM_ADAPTER_CONTRACT_SHA256
from juntai_synthetic_data.worker import SocketWorker
from juntai_synthetic_data.worker_protocol import (
    CancelEnvelope,
    ProtocolError,
    encode_frame,
    read_frame,
)
from juntai_synthetic_data.worker_stream_runtime import (
    STREAM_ADDRESS_ENV,
    STREAM_CA_FILE_ENV,
    STREAM_CLAIM_GENERATION_ENV,
    STREAM_CLAIM_ID_ENV,
    STREAM_CONTRACT_MANIFEST_FILE_ENV,
    STREAM_CONTRACT_MANIFEST_SHA256_ENV,
    STREAM_ENVIRONMENT,
    STREAM_FACTORY,
    STREAM_FACTORY_ENV,
    STREAM_POD_UID_ENV,
    STREAM_PROFILE,
    STREAM_TOKEN_FILE_ENV,
    WorkerSessionBridge,
    WorkerStreamConfiguration,
    _configuration,
    _load_client,
    run_production_worker,
)


def projected_environment(monkeypatch: pytest.MonkeyPatch, tmp_path) -> dict[str, str]:
    ca_file = tmp_path / "ca.crt"
    token_file = tmp_path / "token"
    manifest_file = tmp_path / "manifest.json"
    ca_file.write_text("test-ca")
    ca_file.chmod(0o400)
    token_file.write_text("test-token")
    token_file.chmod(0o400)
    manifest_file.write_text('{"schemaVersion":"test-only"}')
    manifest_file.chmod(0o444)
    from juntai_synthetic_data import worker_stream_runtime

    monkeypatch.setattr(worker_stream_runtime, "STREAM_CA_FILE", str(ca_file))
    monkeypatch.setattr(worker_stream_runtime, "STREAM_TOKEN_FILE", str(token_file))
    monkeypatch.setattr(
        worker_stream_runtime,
        "PLATFORM_ADAPTER_CONTRACT_SHA256",
        hashlib.sha256(manifest_file.read_bytes()).hexdigest(),
    )
    return {
        STREAM_FACTORY_ENV: STREAM_FACTORY,
        STREAM_ADDRESS_ENV: (
            "grpcs://[10.96.0.12]:7443?serverName=swp-executor.juntai-platform.svc.cluster.local"
        ),
        STREAM_CA_FILE_ENV: str(ca_file),
        STREAM_TOKEN_FILE_ENV: str(token_file),
        STREAM_CLAIM_ID_ENV: "claim-opaque-1",
        STREAM_CLAIM_GENERATION_ENV: "3",
        STREAM_POD_UID_ENV: "123e4567-e89b-12d3-a456-426614174000",
        STREAM_CONTRACT_MANIFEST_FILE_ENV: str(manifest_file),
        STREAM_CONTRACT_MANIFEST_SHA256_ENV: hashlib.sha256(manifest_file.read_bytes()).hexdigest(),
    }


def configuration() -> WorkerStreamConfiguration:
    return WorkerStreamConfiguration(
        address=(
            "grpcs://[10.96.0.12]:7443?serverName=swp-executor.juntai-platform.svc.cluster.local"
        ),
        ca_file="/test/ca",
        token_file="/test/token",
        claim_id="claim-opaque-1",
        claim_generation=3,
        pod_uid="123e4567-e89b-12d3-a456-426614174000",
        contract_manifest_file="/test/manifest",
        contract_manifest_sha256=PLATFORM_ADAPTER_CONTRACT_SHA256,
    )


@pytest.mark.parametrize(
    ("error_type", "code", "retryable"),
    [
        (WorkerStreamConfigurationError, "SWP_CONFIG_INVALID", False),
        (WorkerStreamAuthenticationError, "SWP_TLS_AUTHENTICATION_FAILED", False),
        (WorkerStreamAuthorizationError, "SWP_AUTH_REJECTED", False),
        (WorkerStreamClaimError, "SWP_CLAIM_STALE", False),
        (WorkerStreamProtocolError, "SWP_PROTOCOL_INVALID", False),
        (WorkerStreamFrameError, "SWP_FRAME_INVALID", False),
        (WorkerStreamSequenceError, "SWP_SEQUENCE_INVALID", False),
        (WorkerStreamConflictError, "SWP_RESULT_CONFLICT", False),
        (WorkerStreamKeepaliveError, "SWP_KEEPALIVE_EXPIRED", True),
        (WorkerStreamTimeoutError, "SWP_DEADLINE_EXCEEDED", True),
        (WorkerStreamUnavailableError, "SWP_DEPENDENCY_UNAVAILABLE", True),
        (WorkerStreamClosedError, "SWP_SESSION_CLOSED", False),
    ],
)
def test_worker_stream_exception_taxonomy_is_the_exact_platform_contract(
    error_type, code: str, retryable: bool
) -> None:
    error = error_type("redacted test failure")
    assert error.code == code
    assert error.retryable is retryable


def test_exact_nine_worker_projection_is_validated_before_factory(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    environment = projected_environment(monkeypatch, tmp_path)
    assert set(environment) == STREAM_ENVIRONMENT
    value = _configuration(environment)
    assert value.claim_generation == 3
    assert value.pod_uid == environment[STREAM_POD_UID_ENV]

    missing = dict(environment)
    missing.pop(STREAM_CLAIM_ID_ENV)
    with pytest.raises(RuntimeError, match="incomplete"):
        _configuration(missing)
    extra = dict(environment, JUNTAI_SWP_UNAPPROVED="value")
    with pytest.raises(RuntimeError, match="extra"):
        _configuration(extra)
    invalid_generation = dict(environment, JUNTAI_SWP_CLAIM_GENERATION="03")
    with pytest.raises(RuntimeError, match="canonical 1 through 5"):
        _configuration(invalid_generation)
    invalid_address = dict(
        environment,
        JUNTAI_SWP_EXECUTOR_ADDRESS=(
            "grpcs://[127.0.0.1]:7443?serverName=swp-executor.juntai-platform.svc.cluster.local"
        ),
    )
    with pytest.raises(RuntimeError, match="approved executor ClusterIP"):
        _configuration(invalid_address)
    invalid_digest = dict(environment, JUNTAI_SWP_CONTRACT_MANIFEST_SHA256="0" * 64)
    with pytest.raises(RuntimeError, match="not the pinned Platform contract"):
        _configuration(invalid_digest)
    manifest = tmp_path / "manifest.json"
    manifest.chmod(0o644)
    manifest.write_text('{"schemaVersion":"changed-test-only"}')
    manifest.chmod(0o444)
    with pytest.raises(RuntimeError, match="manifest digest mismatch"):
        _configuration(environment)


@pytest.mark.parametrize(
    "address",
    [
        "grpcs://10.96.0.12:7443?serverName=swp-executor.juntai-platform.svc.cluster.local",
        "grpcs://[8.8.8.8]:7443?serverName=swp-executor.juntai-platform.svc.cluster.local",
        "grpcs://[10.96.0.12]:7443?serverName=wrong.juntai-platform.svc.cluster.local",
        (
            "grpcs://[10.96.0.12]:7443?serverName="
            "swp-executor.juntai-platform.svc.cluster.local&fallback=true"
        ),
    ],
)
def test_worker_stream_address_rejects_unbracketed_public_or_extra_components(
    address: str,
) -> None:
    from juntai_synthetic_data.worker_stream_runtime import _validate_address

    with pytest.raises(RuntimeError, match=r"exact grpcs address|approved executor ClusterIP"):
        _validate_address(address)


def test_worker_stream_projection_rejects_symlinked_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path
) -> None:
    environment = projected_environment(monkeypatch, tmp_path)
    token = tmp_path / "token"
    target = tmp_path / "token-target"
    target.write_text("test-token")
    target.chmod(0o400)
    token.unlink()
    token.symlink_to(target)
    with pytest.raises(RuntimeError, match="non-symlink"):
        _configuration(environment)


def test_stream_factory_receives_only_exact_keyword_arguments(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = []
    client = SimpleNamespace(open=lambda **_: None, close=lambda: None)

    def factory(**kwargs):
        calls.append(kwargs)
        return client

    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime.importlib.metadata.version",
        lambda _name: "1.0.0",
    )
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime.importlib.import_module",
        lambda _name: SimpleNamespace(create_worker_client=factory),
    )
    value = configuration()
    assert _load_client(value) is client
    assert calls == [
        {
            "address": value.address,
            "ca_file": value.ca_file,
            "token_file": value.token_file,
            "claim_id": value.claim_id,
            "claim_generation": value.claim_generation,
            "pod_uid": value.pod_uid,
            "contract_manifest_file": value.contract_manifest_file,
            "contract_manifest_sha256": value.contract_manifest_sha256,
        }
    ]


class Session:
    def __init__(self, *, accepted=None, dispatch_frame=None, events=None) -> None:
        config = configuration()
        self.accepted = accepted or Accepted(
            protocol_version=STREAM_PROFILE,
            session_id="session-opaque-1",
            claim_id=config.claim_id,
            claim_generation=config.claim_generation,
            pod_uid=config.pod_uid,
            maximum_payload_bytes=1_048_576,
        )
        self.dispatch_frame = dispatch_frame or encode_frame(dispatch())
        self.events = events
        self.results = []
        self.closed = False

    def receive_dispatch(self, *, timeout_seconds):
        assert timeout_seconds > 0
        return StreamFrame(sequence=1, framed_bytes=self.dispatch_frame)

    def receive_cancel(self, *, timeout_seconds):
        assert timeout_seconds > 0
        time.sleep(0.001)
        return None

    def send_result(self, **kwargs):
        if self.events is not None:
            self.events.append(("send", kwargs["sequence"], kwargs["terminal"]))
        self.results.append(kwargs)

    def close(self):
        if self.events is not None:
            self.events.append(("session-close",))
        self.closed = True


class Client:
    def __init__(self, session: Session, *, events=None) -> None:
        self.session = session
        self.events = events
        self.opened = False
        self.closed = False

    def open(self, *, timeout_seconds):
        assert timeout_seconds > 0
        if self.events is not None:
            self.events.append(("open",))
        self.opened = True
        return self.session

    def close(self):
        if self.events is not None:
            self.events.append(("client-close",))
        self.closed = True


def test_production_bridge_authenticates_before_engine_and_preserves_frames(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session = Session()
    client = Client(session)
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime._configuration",
        lambda _values: configuration(),
    )
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime._load_client", lambda _configuration: client
    )

    def worker_factory() -> SocketWorker:
        assert client.opened
        return SocketWorker(SuccessfulEngine(), workload=EXECUTOR)  # type: ignore[arg-type]

    run_production_worker(worker_factory, environ={})
    assert [item["sequence"] for item in session.results] == [1, 2, 3]
    assert [item["terminal"] for item in session.results] == [False, False, True]
    assert all(item["framed_bytes"][:4] for item in session.results)
    assert session.closed and client.closed


def test_accepted_mismatch_fails_before_engine_construction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    accepted = Accepted(
        protocol_version=STREAM_PROFILE,
        session_id="session-opaque-1",
        claim_id="wrong-claim",
        claim_generation=3,
        pod_uid=configuration().pod_uid,
        maximum_payload_bytes=1_048_576,
    )
    session = Session(accepted=accepted)
    client = Client(session)
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime._configuration",
        lambda _values: configuration(),
    )
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime._load_client", lambda _configuration: client
    )
    built = False

    def worker_factory():
        nonlocal built
        built = True
        raise AssertionError("engine must not be built")

    with pytest.raises(RuntimeError, match="Accepted metadata"):
        run_production_worker(worker_factory, environ={})
    assert not built
    assert session.closed and client.closed


@pytest.mark.parametrize("error_type", [WorkerStreamTimeoutError, WorkerStreamUnavailableError])
def test_production_bridge_reopens_same_claim_and_does_not_reexecute_engine(
    monkeypatch: pytest.MonkeyPatch,
    error_type,
) -> None:
    events = []
    dispatch_frame = encode_frame(dispatch())
    first_session = Session(dispatch_frame=dispatch_frame, events=events)
    second_session = Session(dispatch_frame=dispatch_frame, events=events)
    first_client = Client(first_session, events=events)
    second_client = Client(second_session, events=events)
    first_attempts = []

    def ambiguous_send(**kwargs):
        events.append(("send-ambiguous", kwargs["sequence"], kwargs["terminal"]))
        first_attempts.append(kwargs)
        raise error_type("ambiguous durable acknowledgement")

    first_session.send_result = ambiguous_send
    clients = iter((first_client, second_client))
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime._configuration",
        lambda _values: configuration(),
    )
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime._load_client",
        lambda _configuration: next(clients),
    )
    builds = 0

    def worker_factory() -> SocketWorker:
        nonlocal builds
        builds += 1
        return SocketWorker(SuccessfulEngine(), workload=EXECUTOR)  # type: ignore[arg-type]

    run_production_worker(worker_factory, environ={})

    assert builds == 1
    assert first_attempts == [second_session.results[0]]
    assert [item["sequence"] for item in second_session.results] == [1, 2, 3]
    assert [item["terminal"] for item in second_session.results] == [False, False, True]
    assert events.index(("send", 3, True)) < events.index(("session-close",), 3)
    assert first_session.closed and first_client.closed
    assert second_session.closed and second_client.closed


def test_production_bridge_reconnect_rejects_nonidentical_dispatch_before_resend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    original = dispatch()
    original_frame = encode_frame(original)
    changed = original.model_copy(update={"correlation_id": "different-correlation"}).signed()
    changed_frame = encode_frame(changed)
    assert original_frame != changed_frame
    first_session = Session(dispatch_frame=original_frame)
    second_session = Session(dispatch_frame=changed_frame)
    first_session.send_result = lambda **_kwargs: (_ for _ in ()).throw(
        WorkerStreamTimeoutError("ambiguous durable acknowledgement")
    )
    clients = iter((Client(first_session), Client(second_session)))
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime._configuration",
        lambda _values: configuration(),
    )
    monkeypatch.setattr(
        "juntai_synthetic_data.worker_stream_runtime._load_client",
        lambda _configuration: next(clients),
    )

    with pytest.raises(ProtocolError, match="byte-identical"):
        run_production_worker(
            lambda: SocketWorker(SuccessfulEngine(), workload=EXECUTOR),  # type: ignore[arg-type]
            environ={},
        )
    assert first_session.closed and second_session.closed
    assert second_session.results == []


@pytest.mark.parametrize("error_type", [WorkerStreamTimeoutError, WorkerStreamUnavailableError])
def test_result_ambiguous_failure_reauthenticates_and_replays_only_identical_tuple(
    error_type,
) -> None:
    first = Session()
    second = Session(dispatch_frame=first.dispatch_frame)
    first_attempts = []

    def send_result(**kwargs):
        first_attempts.append(kwargs)
        raise error_type("ambiguous acknowledgement")

    first.send_result = send_result
    reconnects = []

    def reconnect():
        reconnects.append(True)
        return second

    bridge = WorkerSessionBridge(first, first_inbound_sequence=1, reconnect=reconnect)
    event_worker = SocketWorker(SuccessfulEngine(), workload=EXECUTOR)  # type: ignore[arg-type]
    message = dispatch()
    started = event_worker._event(message, "STARTED", "RUNNING", 0).signed()
    frame = encode_frame(started)
    bridge.sendall(frame)
    assert reconnects == [True]
    assert first_attempts == second.results
    assert first_attempts[0]["sequence"] == 1
    assert first_attempts[0]["framed_bytes"] == frame
    assert first_attempts[0]["terminal"] is False


def test_result_non_timeout_stream_error_is_not_retried() -> None:
    session = Session()
    attempts = []

    def send_result(**kwargs):
        attempts.append(kwargs)
        raise WorkerStreamAuthenticationError("peer rejected certificate")

    session.send_result = send_result
    bridge = WorkerSessionBridge(session, first_inbound_sequence=1)
    event_worker = SocketWorker(SuccessfulEngine(), workload=EXECUTOR)  # type: ignore[arg-type]
    message = dispatch()
    started = event_worker._event(message, "STARTED", "RUNNING", 0).signed()
    with pytest.raises(WorkerStreamAuthenticationError):
        bridge.sendall(encode_frame(started))
    assert len(attempts) == 1


def cancel_frame() -> bytes:
    message = dispatch()
    now = datetime.now(UTC)
    cancellation = CancelEnvelope(
        messageId="cancel-stream-1",
        tenantId=message.tenant_id,
        jobId=message.job_id,
        attemptId=message.attempt_id,
        attemptNumber=message.attempt_number,
        sequence=1,
        emittedAt=now,
        deadline=now + timedelta(minutes=1),
        correlationId=message.correlation_id,
        producerWorkload=message.producer_workload,
        cancelSequence=1,
        requestedAt=now,
        reasonCode="caller-requested",
        requestedByKind="delegated",
        graceDeadline=now + timedelta(seconds=30),
    ).signed()
    return encode_frame(cancellation)


def test_session_bridge_accepts_only_contiguous_cancel_transport_sequences() -> None:
    class CancelSession(Session):
        def __init__(self, sequence: int, framed_bytes: bytes) -> None:
            super().__init__()
            self.frame = StreamFrame(sequence=sequence, framed_bytes=framed_bytes)

        def receive_cancel(self, *, timeout_seconds):
            del timeout_seconds
            value, self.frame = self.frame, None
            return value

    session = CancelSession(2, cancel_frame())
    bridge = WorkerSessionBridge(session, first_inbound_sequence=1)
    cancellation = read_frame(bridge)  # type: ignore[arg-type]
    assert isinstance(cancellation, CancelEnvelope)
    assert cancellation.cancel_sequence == 1

    gap = WorkerSessionBridge(CancelSession(3, cancel_frame()), first_inbound_sequence=1)
    with pytest.raises(ProtocolError, match="truncated"):
        read_frame(gap)  # type: ignore[arg-type]
    assert isinstance(gap.error, ProtocolError)

    wrong_envelope = WorkerSessionBridge(
        CancelSession(2, encode_frame(dispatch())), first_inbound_sequence=1
    )
    with pytest.raises(ProtocolError, match="truncated"):
        read_frame(wrong_envelope)  # type: ignore[arg-type]
    assert isinstance(wrong_envelope.error, ProtocolError)


def test_session_bridge_treats_peer_close_before_terminal_as_failure() -> None:
    session = Session()
    session.receive_cancel = lambda **_kwargs: None
    bridge = WorkerSessionBridge(session, first_inbound_sequence=1)
    with pytest.raises(ProtocolError, match="truncated"):
        read_frame(bridge)  # type: ignore[arg-type]
    assert isinstance(bridge.error, WorkerStreamClosedError)
