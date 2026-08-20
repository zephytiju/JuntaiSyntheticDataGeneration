"""Production composition for the Platform-owned SWP worker stream."""

from __future__ import annotations

import hashlib
import importlib
import importlib.metadata
import ipaddress
import os
import re
import stat
import struct
import threading
import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path

from juntai_platform_swp_stream import (
    Accepted,
    WorkerClient,
    WorkerSession,
    WorkerStreamClosedError,
    WorkerStreamTimeoutError,
    WorkerStreamUnavailableError,
)

from juntai_synthetic_data.platform_adapter_contract import PLATFORM_ADAPTER_CONTRACT_SHA256
from juntai_synthetic_data.worker import SocketWorker, validate_worker_isolation
from juntai_synthetic_data.worker_protocol import (
    MAX_FRAME_BYTES,
    CancelEnvelope,
    DispatchEnvelope,
    ProtocolError,
    WorkerEventEnvelope,
    decode_envelope,
)

STREAM_FACTORY = "juntai_platform_swp_stream:create_worker_client"
STREAM_DISTRIBUTION = "juntai-platform-swp-stream"
STREAM_VERSION = "1.0.0"
STREAM_PROFILE = "juntai.platform.swp-stream/v1"
STREAM_SERVER_NAME = "swp-executor.juntai-platform.svc.cluster.local"
STREAM_CA_FILE = "/var/run/secrets/juntai/swp/ca.crt"
STREAM_TOKEN_FILE = "/var/run/secrets/juntai/swp/token"

STREAM_FACTORY_ENV = "JUNTAI_SWP_TRANSPORT_FACTORY"
STREAM_ADDRESS_ENV = "JUNTAI_SWP_EXECUTOR_ADDRESS"
STREAM_CA_FILE_ENV = "JUNTAI_SWP_EXECUTOR_CA_FILE"
STREAM_TOKEN_FILE_ENV = "JUNTAI_SWP_WORKLOAD_TOKEN_FILE"
STREAM_CLAIM_ID_ENV = "JUNTAI_SWP_CLAIM_ID"
STREAM_CLAIM_GENERATION_ENV = "JUNTAI_SWP_CLAIM_GENERATION"
STREAM_POD_UID_ENV = "JUNTAI_SWP_POD_UID"
STREAM_CONTRACT_MANIFEST_FILE_ENV = "JUNTAI_SWP_CONTRACT_MANIFEST_FILE"
STREAM_CONTRACT_MANIFEST_SHA256_ENV = "JUNTAI_SWP_CONTRACT_MANIFEST_SHA256"

STREAM_ENVIRONMENT = frozenset(
    {
        STREAM_FACTORY_ENV,
        STREAM_ADDRESS_ENV,
        STREAM_CA_FILE_ENV,
        STREAM_TOKEN_FILE_ENV,
        STREAM_CLAIM_ID_ENV,
        STREAM_CLAIM_GENERATION_ENV,
        STREAM_POD_UID_ENV,
        STREAM_CONTRACT_MANIFEST_FILE_ENV,
        STREAM_CONTRACT_MANIFEST_SHA256_ENV,
    }
)

_ASCII = re.compile(r"^[\x21-\x7e]{1,200}$")
_DIGEST = re.compile(r"^[0-9a-f]{64}$")
_CANONICAL_GENERATION = re.compile(r"^[1-5]$")
_SESSION_ID = re.compile(r"^[\x21-\x7e]{1,200}$")
_ADDRESS = re.compile(
    r"^grpcs://\[(?P<ip>[^\]]+)\]:7443\?serverName=" + re.escape(STREAM_SERVER_NAME) + r"$"
)
_OPEN_TIMEOUT_SECONDS = 30.0
_RECEIVE_TIMEOUT_SECONDS = 20.0
_SEND_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True)
class WorkerStreamConfiguration:
    address: str
    ca_file: str
    token_file: str
    claim_id: str
    claim_generation: int
    pod_uid: str
    contract_manifest_file: str
    contract_manifest_sha256: str


def _projection_file(
    value: str,
    *,
    name: str,
    exact_path: str | None = None,
    exact_mode: int | None = None,
    maximum_bytes: int = 1_048_576,
) -> Path:
    if exact_path is not None and value != exact_path:
        raise RuntimeError(f"{name} must be exactly {exact_path}")
    path = Path(value)
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise RuntimeError(f"{name} must name an existing absolute non-symlink regular file")
    file_stat = path.stat()
    mode = stat.S_IMODE(file_stat.st_mode)
    if exact_mode is not None and mode != exact_mode:
        raise RuntimeError(f"{name} must have mode {exact_mode:04o}")
    if exact_mode is None and mode & 0o222:
        raise RuntimeError(f"{name} must be read-only")
    if not 1 <= file_stat.st_size <= maximum_bytes:
        raise RuntimeError(f"{name} exceeds its bounded projection")
    return path


def _validate_address(value: str) -> str:
    match = _ADDRESS.fullmatch(value)
    if match is None:
        raise RuntimeError(f"{STREAM_ADDRESS_ENV} is not an exact grpcs address")
    try:
        address = ipaddress.ip_address(match.group("ip"))
    except ValueError as error:
        raise RuntimeError(f"{STREAM_ADDRESS_ENV} is not an exact grpcs address") from error
    if (
        not address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_unspecified
        or address.is_reserved
    ):
        raise RuntimeError(f"{STREAM_ADDRESS_ENV} is not the approved executor ClusterIP")
    return value


def _configuration(environ: Mapping[str, str] | None = None) -> WorkerStreamConfiguration:
    values = dict(os.environ if environ is None else environ)
    present = {name for name in values if name.startswith("JUNTAI_SWP_")}
    if present != STREAM_ENVIRONMENT:
        missing = sorted(STREAM_ENVIRONMENT - present)
        extra = sorted(present - STREAM_ENVIRONMENT)
        raise RuntimeError(
            "worker stream projection is incomplete or has extra names: "
            f"missing={','.join(missing)} extra={','.join(extra)}"
        )
    if values[STREAM_FACTORY_ENV] != STREAM_FACTORY:
        raise RuntimeError(f"{STREAM_FACTORY_ENV} must be exactly {STREAM_FACTORY}")
    address = _validate_address(values[STREAM_ADDRESS_ENV])
    ca_file = _projection_file(
        values[STREAM_CA_FILE_ENV], name=STREAM_CA_FILE_ENV, exact_path=STREAM_CA_FILE
    )
    token_file = _projection_file(
        values[STREAM_TOKEN_FILE_ENV],
        name=STREAM_TOKEN_FILE_ENV,
        exact_path=STREAM_TOKEN_FILE,
        exact_mode=0o400,
        maximum_bytes=16_384,
    )
    claim_id = values[STREAM_CLAIM_ID_ENV]
    if not _ASCII.fullmatch(claim_id):
        raise RuntimeError(f"{STREAM_CLAIM_ID_ENV} must be bounded printable ASCII")
    generation_value = values[STREAM_CLAIM_GENERATION_ENV]
    if not _CANONICAL_GENERATION.fullmatch(generation_value):
        raise RuntimeError(f"{STREAM_CLAIM_GENERATION_ENV} must be canonical 1 through 5")
    pod_uid = values[STREAM_POD_UID_ENV]
    try:
        parsed_uid = uuid.UUID(pod_uid)
    except ValueError as error:
        raise RuntimeError(f"{STREAM_POD_UID_ENV} must be a canonical lowercase UUID") from error
    if str(parsed_uid) != pod_uid:
        raise RuntimeError(f"{STREAM_POD_UID_ENV} must be a canonical lowercase UUID")
    manifest_file = _projection_file(
        values[STREAM_CONTRACT_MANIFEST_FILE_ENV],
        name=STREAM_CONTRACT_MANIFEST_FILE_ENV,
    )
    manifest_digest = values[STREAM_CONTRACT_MANIFEST_SHA256_ENV]
    if not _DIGEST.fullmatch(manifest_digest):
        raise RuntimeError(
            f"{STREAM_CONTRACT_MANIFEST_SHA256_ENV} must be 64 lowercase hexadecimal"
        )
    if manifest_digest != PLATFORM_ADAPTER_CONTRACT_SHA256:
        raise RuntimeError("worker stream contract manifest is not the pinned Platform contract")
    if hashlib.sha256(manifest_file.read_bytes()).hexdigest() != manifest_digest:
        raise RuntimeError("worker stream contract manifest digest mismatch")
    return WorkerStreamConfiguration(
        address=address,
        ca_file=str(ca_file),
        token_file=str(token_file),
        claim_id=claim_id,
        claim_generation=int(generation_value),
        pod_uid=pod_uid,
        contract_manifest_file=str(manifest_file),
        contract_manifest_sha256=manifest_digest,
    )


def _load_client(configuration: WorkerStreamConfiguration) -> WorkerClient:
    try:
        installed_version = importlib.metadata.version(STREAM_DISTRIBUTION)
    except importlib.metadata.PackageNotFoundError as error:
        raise RuntimeError(f"required worker stream {STREAM_DISTRIBUTION} is absent") from error
    if installed_version != STREAM_VERSION:
        raise RuntimeError(
            f"required worker stream version is {STREAM_VERSION}, found {installed_version}"
        )
    module_name, attribute = STREAM_FACTORY.split(":", 1)
    factory = getattr(importlib.import_module(module_name), attribute, None)
    if not callable(factory):
        raise RuntimeError("worker stream factory is not callable")
    client = factory(
        address=configuration.address,
        ca_file=configuration.ca_file,
        token_file=configuration.token_file,
        claim_id=configuration.claim_id,
        claim_generation=configuration.claim_generation,
        pod_uid=configuration.pod_uid,
        contract_manifest_file=configuration.contract_manifest_file,
        contract_manifest_sha256=configuration.contract_manifest_sha256,
    )
    if not callable(getattr(client, "open", None)) or not callable(getattr(client, "close", None)):
        raise RuntimeError("worker stream client does not implement the required contract")
    return client


def _complete_frame(value: bytes) -> DispatchEnvelope | CancelEnvelope | WorkerEventEnvelope:
    if not isinstance(value, bytes) or len(value) < 5:
        raise ProtocolError("ENVELOPE_INVALID", "stream data is not one complete SWP frame")
    (length,) = struct.unpack(">I", value[:4])
    if length < 1 or length > MAX_FRAME_BYTES or len(value) != length + 4:
        raise ProtocolError("ENVELOPE_INVALID", "stream SWP frame length is invalid")
    return decode_envelope(value[4:])


def _validate_accepted(accepted: Accepted, configuration: WorkerStreamConfiguration) -> None:
    if (
        getattr(accepted, "protocol_version", None) != STREAM_PROFILE
        or getattr(accepted, "claim_id", None) != configuration.claim_id
        or getattr(accepted, "claim_generation", None) != configuration.claim_generation
        or getattr(accepted, "pod_uid", None) != configuration.pod_uid
        or getattr(accepted, "maximum_payload_bytes", None) != MAX_FRAME_BYTES
        or not _SESSION_ID.fullmatch(str(getattr(accepted, "session_id", "")))
    ):
        raise RuntimeError("worker stream Accepted metadata does not match the configured claim")


class WorkerSessionBridge:
    """Expose only recv/sendall framing behavior over an authenticated WorkerSession."""

    def __init__(
        self,
        session: WorkerSession,
        *,
        first_inbound_sequence: int,
        reconnect: Callable[[], WorkerSession] | None = None,
    ) -> None:
        self._session: WorkerSession | None = session
        self._session_generation = 0
        self._session_condition = threading.Condition()
        self._reconnect = reconnect
        self._inbound_sequence = first_inbound_sequence
        self._outbound_sequence = 1
        self._receive_buffer = b""
        self._terminal_sent = False
        self._terminal_pending = False
        self._error: BaseException | None = None

    @property
    def terminal_sent(self) -> bool:
        return self._terminal_sent

    @property
    def error(self) -> BaseException | None:
        with self._session_condition:
            return self._error

    def _session_snapshot(self) -> tuple[WorkerSession, int] | None:
        with self._session_condition:
            while self._session is None and self._error is None and not self._terminal_sent:
                self._session_condition.wait()
            if self._session is None:
                return None
            return self._session, self._session_generation

    def _latch_error(self, error: BaseException) -> None:
        with self._session_condition:
            if self._error is None:
                self._error = error
            self._session_condition.notify_all()

    def _replace_session(self) -> WorkerSession:
        if self._reconnect is None:
            raise RuntimeError("worker stream reconnect is unavailable")
        with self._session_condition:
            self._session = None
            self._session_generation += 1
            self._session_condition.notify_all()
        session = self._reconnect()
        with self._session_condition:
            self._session = session
            self._inbound_sequence = 1
            self._session_condition.notify_all()
        return session

    def _next_cancel(self) -> bytes:
        while not self._terminal_sent:
            snapshot = self._session_snapshot()
            if snapshot is None:
                return b""
            session, generation = snapshot
            try:
                frame = session.receive_cancel(timeout_seconds=_RECEIVE_TIMEOUT_SECONDS)
            except BaseException as error:
                with self._session_condition:
                    if generation != self._session_generation or session is not self._session:
                        continue
                self._latch_error(error)
                return b""
            if frame is None:
                with self._session_condition:
                    if generation != self._session_generation or session is not self._session:
                        continue
                    terminal = self._terminal_pending or self._terminal_sent
                if terminal:
                    return b""
                self._latch_error(
                    WorkerStreamClosedError(
                        "worker Attach closed before the terminal result was accepted"
                    )
                )
                return b""
            with self._session_condition:
                if generation != self._session_generation or session is not self._session:
                    continue
            sequence = getattr(frame, "sequence", None)
            framed_bytes = getattr(frame, "framed_bytes", None)
            if sequence != self._inbound_sequence + 1:
                self._latch_error(
                    ProtocolError(
                        "ENVELOPE_INVALID", "worker stream cancellation sequence is not contiguous"
                    )
                )
                return b""
            try:
                envelope = _complete_frame(framed_bytes)
            except BaseException as error:
                self._latch_error(error)
                return b""
            if not isinstance(envelope, CancelEnvelope):
                self._latch_error(
                    ProtocolError(
                        "ENVELOPE_INVALID",
                        "worker stream cancellation data is not a cancel envelope",
                    )
                )
                return b""
            self._inbound_sequence = sequence
            return framed_bytes
        return b""

    def recv(self, length: int) -> bytes:
        if length <= 0:
            return b""
        if not self._receive_buffer:
            self._receive_buffer = self._next_cancel()
        chunk = self._receive_buffer[:length]
        self._receive_buffer = self._receive_buffer[length:]
        return chunk

    def sendall(self, framed_bytes: bytes) -> None:
        with self._session_condition:
            error = self._error
        if error is not None:
            raise RuntimeError("worker stream failed while awaiting control") from error
        if self._terminal_sent:
            raise ProtocolError("ENVELOPE_INVALID", "worker emitted data after its terminal result")
        envelope = _complete_frame(framed_bytes)
        if not isinstance(envelope, WorkerEventEnvelope):
            raise ProtocolError("ENVELOPE_INVALID", "worker emitted a non-result stream frame")
        terminal = envelope.event_type == "TERMINAL"
        arguments = {
            "sequence": self._outbound_sequence,
            "framed_bytes": framed_bytes,
            "terminal": terminal,
            "timeout_seconds": _SEND_TIMEOUT_SECONDS,
        }
        with self._session_condition:
            self._terminal_pending = terminal
        snapshot = self._session_snapshot()
        if snapshot is None:
            raise RuntimeError("worker stream session is unavailable")
        session, _ = snapshot
        try:
            session.send_result(**arguments)
        except (WorkerStreamTimeoutError, WorkerStreamUnavailableError):
            try:
                session = self._replace_session()
                session.send_result(**arguments)
            except BaseException as error:
                self._latch_error(error)
                raise
        except BaseException as error:
            self._latch_error(error)
            raise
        with self._session_condition:
            self._outbound_sequence += 1
            self._terminal_sent = terminal
            self._terminal_pending = False
            self._session_condition.notify_all()


@dataclass
class _ClaimConnection:
    client: WorkerClient
    session: WorkerSession
    dispatch_frame: bytes
    dispatch: DispatchEnvelope

    def close(self) -> None:
        try:
            self.session.close()
        finally:
            self.client.close()


def _open_claim_connection(configuration: WorkerStreamConfiguration) -> _ClaimConnection:
    client = _load_client(configuration)
    session = None
    try:
        session = client.open(timeout_seconds=_OPEN_TIMEOUT_SECONDS)
        required = ("receive_dispatch", "receive_cancel", "send_result", "close")
        if any(not callable(getattr(session, method, None)) for method in required):
            raise RuntimeError("worker stream session does not implement the required contract")
        _validate_accepted(session.accepted, configuration)
        first = session.receive_dispatch(timeout_seconds=_RECEIVE_TIMEOUT_SECONDS)
        if getattr(first, "sequence", None) != 1:
            raise ProtocolError("ENVELOPE_INVALID", "first worker stream sequence is not one")
        dispatch_frame = getattr(first, "framed_bytes", None)
        dispatch = _complete_frame(dispatch_frame)
        if not isinstance(dispatch, DispatchEnvelope):
            raise ProtocolError("ENVELOPE_INVALID", "first worker stream frame is not dispatch")
        return _ClaimConnection(client, session, dispatch_frame, dispatch)
    except BaseException:
        try:
            if session is not None:
                session.close()
        finally:
            client.close()
        raise


def run_production_worker(
    worker_factory: Callable[[], SocketWorker],
    *,
    environ: Mapping[str, str] | None = None,
) -> None:
    """Validate, authenticate, then construct and run one fenced worker claim."""

    values = dict(os.environ if environ is None else environ)
    validate_worker_isolation(values)
    configuration = _configuration(values)
    connection: _ClaimConnection | None = None
    try:
        connection = _open_claim_connection(configuration)
        original_dispatch_frame = connection.dispatch_frame

        def reconnect() -> WorkerSession:
            nonlocal connection
            assert connection is not None
            previous, connection = connection, None
            previous.close()
            candidate = _open_claim_connection(configuration)
            if candidate.dispatch_frame != original_dispatch_frame:
                candidate.close()
                raise ProtocolError(
                    "ENVELOPE_INVALID",
                    "reconnected worker stream dispatch is not byte-identical",
                )
            connection = candidate
            return candidate.session

        bridge = WorkerSessionBridge(
            connection.session,
            first_inbound_sequence=1,
            reconnect=reconnect,
        )
        worker = worker_factory()
        worker.process(bridge, connection.dispatch)  # type: ignore[arg-type]
        if bridge.error is not None:
            raise RuntimeError("worker stream failed during execution") from bridge.error
        if not bridge.terminal_sent:
            raise ProtocolError("ENVELOPE_INVALID", "worker did not emit one terminal result")
    finally:
        if connection is not None:
            connection.close()
