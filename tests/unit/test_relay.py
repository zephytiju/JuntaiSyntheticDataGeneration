from __future__ import annotations

import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest
from juntai_platform_queue_kafka import (
    AuthenticatedProducer,
    PublicationReceipt,
    QueueAuthenticationError,
    QueueAuthorizationError,
    QueueBindingMetadata,
    QueueConfigurationError,
    QueueConflictError,
    QueueContractError,
    QueueLeaseLostError,
    QueueReceiptError,
    QueueTimeoutError,
    QueueUnavailableError,
    ReceivedMessage,
)
from test_worker_coordinator import API, EXECUTOR, WORKER, event, queued

from juntai_synthetic_data import relay_runtime
from juntai_synthetic_data.execution.coordinator import (
    CONTROL_CHANNEL,
    DEAD_LETTER_CHANNEL,
    DISPATCH_CHANNEL,
    RESULT_CHANNEL,
)
from juntai_synthetic_data.jobs import JobState
from juntai_synthetic_data.relay.models import (
    KAFKA_IMAGE_DIGEST,
    KAFKA_PRODUCT,
    KAFKA_VERSION,
    QUEUE_BINDING_PROFILE,
)
from juntai_synthetic_data.relay.runner import SyntheticRelay, outbox_retry_delay
from juntai_synthetic_data.relay.transport import (
    REQUIRED_CHANNELS,
    QueueBindingError,
    ordering_key_for,
    validate_binding_metadata,
    validate_publish_ordering_key,
)
from juntai_synthetic_data.relay_runtime import (
    CONTRACT_MANIFEST_FILE_ENV,
    CONTRACT_MANIFEST_SHA256_ENV,
    ENDPOINT_ENV,
    QUEUE_CREDENTIAL_SCHEMA,
    QUEUE_ENVIRONMENT,
    QUEUE_SERVER_NAME,
    TOKEN_FILE_ENV,
    TRANSPORT_FACTORY_ENV,
    _capability_configuration,
    _transport,
    _validate_credential_binding,
)
from juntai_synthetic_data.worker_protocol import ProtocolError

RECEIPT = "A" * 43
DLQ_RECEIPT = "B" * 43
CONFLICT_RECEIPT = "C" * 43
PLATFORM_API = AuthenticatedProducer(API.namespace, API.service_account)
PLATFORM_EXECUTOR = AuthenticatedProducer(EXECUTOR.namespace, EXECUTOR.service_account)
PLATFORM_WORKER = AuthenticatedProducer(WORKER.namespace, WORKER.service_account)
QUEUE_CLUSTER_IP = "10.96.0.42"


def endpoint(channel: str, *, address: str = QUEUE_CLUSTER_IP) -> str:
    return f"grpcs://[{address}]:7444/{channel}?serverName={QUEUE_SERVER_NAME}"


def metadata() -> QueueBindingMetadata:
    return QueueBindingMetadata(
        schema_version=QUEUE_BINDING_PROFILE,
        channels=REQUIRED_CHANNELS,
        provider=KAFKA_PRODUCT,
        provider_version=KAFKA_VERSION,
        provider_image_digest=KAFKA_IMAGE_DIGEST,
        delivery_semantics="at-least-once-platform-ledger-authoritative",
        visibility_seconds=60,
        renew_every_seconds=20,
        maximum_deliveries=5,
        idempotent_publish=True,
        dead_letter=True,
    )


class Queue:
    def __init__(self) -> None:
        self.deliveries: dict[str, list[ReceivedMessage]] = {
            RESULT_CHANNEL: [],
            DEAD_LETTER_CHANNEL: [],
        }
        self.publications: list[tuple[str, str, str, bytes, str]] = []
        self.settlements: list[tuple[str, str, float | None]] = []
        self.renewals = 0
        self.metadata_calls = 0
        self.before_ack = None

    def binding_metadata(self):
        self.metadata_calls += 1
        return metadata()

    def publish(self, channel, *, message_id, content_digest, canonical_bytes, ordering_key):
        identity = (channel, message_id, content_digest, canonical_bytes, ordering_key)
        self.publications.append(identity)
        return PublicationReceipt(message_id, content_digest, "broker-" + message_id)

    def receive(self, channel, *, limit, visibility_seconds):
        assert visibility_seconds == 60
        values = tuple(self.deliveries[channel][:limit])
        del self.deliveries[channel][:limit]
        return values

    def renew(self, receipt, *, visibility_seconds):
        assert receipt
        assert 1 <= visibility_seconds <= 60
        self.renewals += 1

    def acknowledge(self, receipt):
        if self.before_ack:
            self.before_ack()
        self.settlements.append(("ack", receipt, None))

    def release(self, receipt):
        self.settlements.append(("release", receipt, None))

    def reject(self, receipt, *, reason_code):
        self.settlements.append(("reject", receipt, None))


def delivery(envelope, *, producer=PLATFORM_EXECUTOR, delivery_count=1) -> ReceivedMessage:
    return ReceivedMessage(
        channel=RESULT_CHANNEL,
        message_id=envelope.message_id,
        content_digest=envelope.content_digest,
        canonical_bytes=envelope.canonical_bytes(),
        receipt=RECEIPT,
        delivery_count=delivery_count,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        authenticated_producer=producer,
    )


def test_transport_binding_fails_closed_without_exact_platform_metadata() -> None:
    validate_binding_metadata(metadata())
    with pytest.raises(QueueBindingError, match="absent"):
        validate_binding_metadata(None)
    with pytest.raises(QueueBindingError, match="incompatible"):
        validate_binding_metadata(replace(metadata(), provider_version="4.1.0"))
    with pytest.raises(QueueBindingError, match="incompatible"):
        validate_binding_metadata(replace(metadata(), maximum_deliveries=6))
    with pytest.raises(QueueBindingError, match="incompatible"):
        validate_binding_metadata(replace(metadata(), provider_image_digest="sha256:" + "0" * 64))


@pytest.mark.parametrize(
    ("error_type", "code", "retryable", "permanent"),
    [
        (QueueConfigurationError, "QUEUE_CONFIG_INVALID", False, True),
        (QueueContractError, "QUEUE_CONTRACT_MISMATCH", False, True),
        (QueueAuthenticationError, "QUEUE_AUTHENTICATION_FAILED", False, True),
        (QueueAuthorizationError, "QUEUE_AUTHORIZATION_DENIED", False, True),
        (QueueConflictError, "QUEUE_PUBLICATION_CONFLICT", False, True),
        (QueueReceiptError, "QUEUE_RECEIPT_INVALID", False, True),
        (QueueLeaseLostError, "QUEUE_LEASE_LOST", False, True),
        (QueueUnavailableError, "QUEUE_DEPENDENCY_UNAVAILABLE", True, False),
        (QueueTimeoutError, "QUEUE_DEADLINE_EXCEEDED", True, False),
    ],
)
def test_queue_exception_taxonomy_is_imported_without_local_reinterpretation(
    error_type, code: str, retryable: bool, permanent: bool
) -> None:
    error = error_type("redacted test failure")
    assert error.code == code
    assert error.retryable is retryable
    assert SyntheticRelay._reason(error) == code
    assert SyntheticRelay._is_permanent(error) is permanent


def test_queue_capability_configuration_requires_existing_projected_credential(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    files = {}
    for name in ("ca", "certificate", "private-key"):
        path = tmp_path / name
        path.write_text("test-only")
        files[name] = path
    credential = tmp_path / "credentials.json"
    credential.write_text(
        json.dumps(
            {
                "schemaVersion": QUEUE_CREDENTIAL_SCHEMA,
                "caFile": str(files["ca"]),
                "certificateFile": str(files["certificate"]),
                "privateKeyFile": str(files["private-key"]),
                "serverName": QUEUE_SERVER_NAME,
                "notBefore": "2026-08-19T00:00:00Z",
                "notAfter": "2026-08-20T00:00:00Z",
            }
        )
    )
    credential.chmod(0o400)
    _validate_credential_binding(credential)
    contract_manifest = tmp_path / "manifest.json"
    contract_manifest.write_bytes(b'{"schemaVersion":"test-only"}')
    contract_manifest.chmod(0o400)
    import hashlib

    contract_digest = hashlib.sha256(contract_manifest.read_bytes()).hexdigest()
    monkeypatch.setattr(relay_runtime, "PLATFORM_ADAPTER_CONTRACT_SHA256", contract_digest)
    for channel, name in ENDPOINT_ENV.items():
        monkeypatch.setenv(name, endpoint(channel))
    monkeypatch.setenv(TOKEN_FILE_ENV, str(credential))
    monkeypatch.setenv(CONTRACT_MANIFEST_FILE_ENV, str(contract_manifest))
    monkeypatch.setenv(CONTRACT_MANIFEST_SHA256_ENV, contract_digest)
    monkeypatch.setenv(TRANSPORT_FACTORY_ENV, relay_runtime.TRANSPORT_FACTORY)
    assert {
        name for name in relay_runtime.os.environ if name.startswith("JUNTAI_QUEUE_")
    } == QUEUE_ENVIRONMENT
    with pytest.raises(RuntimeError, match="must be exactly"):
        _capability_configuration()
    monkeypatch.setattr(relay_runtime, "QUEUE_CREDENTIAL_FILE", str(credential))
    configuration = _capability_configuration()
    assert configuration.credential_file == str(credential)
    assert configuration.endpoints == {channel: endpoint(channel) for channel in ENDPOINT_ENV}
    assert configuration.contract_manifest_file == str(contract_manifest)
    assert configuration.contract_manifest_sha256 == contract_digest
    monkeypatch.setenv(
        ENDPOINT_ENV[CONTROL_CHANNEL],
        endpoint(DISPATCH_CHANNEL),
    )
    with pytest.raises(RuntimeError, match="exact executor QueueTransport endpoint"):
        _capability_configuration()

    monkeypatch.setenv(ENDPOINT_ENV[CONTROL_CHANNEL], endpoint(CONTROL_CHANNEL))
    monkeypatch.setenv(
        ENDPOINT_ENV[CONTROL_CHANNEL], endpoint(CONTROL_CHANNEL, address="10.96.0.43")
    )
    with pytest.raises(RuntimeError, match="share one executor authority"):
        _capability_configuration()

    monkeypatch.setenv(ENDPOINT_ENV[CONTROL_CHANNEL], endpoint(CONTROL_CHANNEL))
    monkeypatch.setenv(CONTRACT_MANIFEST_SHA256_ENV, "0" * 64)
    with pytest.raises(RuntimeError, match="not the pinned Platform contract"):
        _capability_configuration()
    monkeypatch.setenv(CONTRACT_MANIFEST_SHA256_ENV, contract_digest)
    contract_manifest.chmod(0o600)
    contract_manifest.write_bytes(b'{"schemaVersion":"changed-test-only"}')
    contract_manifest.chmod(0o400)
    with pytest.raises(RuntimeError, match="manifest digest mismatch"):
        _capability_configuration()
    contract_manifest.chmod(0o600)
    contract_manifest.write_bytes(b'{"schemaVersion":"test-only"}')
    contract_manifest.chmod(0o400)
    monkeypatch.setenv("JUNTAI_QUEUE_UNAPPROVED", "value")
    with pytest.raises(RuntimeError, match="extra names"):
        _capability_configuration()


def test_queue_credential_and_factory_identity_fail_closed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    binding = tmp_path / "credentials.json"
    binding.write_text("{}")
    binding.chmod(0o600)
    with pytest.raises(RuntimeError, match="mode-0400"):
        _validate_credential_binding(binding)
    binding.chmod(0o400)
    with pytest.raises(RuntimeError, match="fields are incompatible"):
        _validate_credential_binding(binding)
    binding_target = tmp_path / "credential-target.json"
    binding_target.write_text("{}")
    binding_target.chmod(0o400)
    binding.unlink()
    binding.symlink_to(binding_target)
    with pytest.raises(RuntimeError, match="non-symlink"):
        _validate_credential_binding(binding)
    monkeypatch.setenv(TRANSPORT_FACTORY_ENV, "unapproved:create_transport")
    with pytest.raises(RuntimeError, match="must be exactly"):
        _transport()


def test_queue_factory_is_keyword_only_and_validated_before_service_composition(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    files = {}
    for name in ("ca", "certificate", "private-key"):
        path = tmp_path / name
        path.write_text("test-only")
        files[name] = path
    credential = tmp_path / "credentials.json"
    credential.write_text(
        json.dumps(
            {
                "schemaVersion": QUEUE_CREDENTIAL_SCHEMA,
                "caFile": str(files["ca"]),
                "certificateFile": str(files["certificate"]),
                "privateKeyFile": str(files["private-key"]),
                "serverName": QUEUE_SERVER_NAME,
                "notBefore": "2026-08-20T00:00:00Z",
                "notAfter": "2026-08-21T00:00:00Z",
            }
        )
    )
    credential.chmod(0o400)
    manifest = tmp_path / "manifest.json"
    manifest.write_text('{"schemaVersion":"test-only"}')
    manifest.chmod(0o400)
    import hashlib

    for channel, name in ENDPOINT_ENV.items():
        monkeypatch.setenv(name, endpoint(channel))
    monkeypatch.setenv(TRANSPORT_FACTORY_ENV, relay_runtime.TRANSPORT_FACTORY)
    monkeypatch.setenv(TOKEN_FILE_ENV, str(credential))
    monkeypatch.setenv(CONTRACT_MANIFEST_FILE_ENV, str(manifest))
    monkeypatch.setenv(
        CONTRACT_MANIFEST_SHA256_ENV, hashlib.sha256(manifest.read_bytes()).hexdigest()
    )
    monkeypatch.setattr(
        relay_runtime,
        "PLATFORM_ADAPTER_CONTRACT_SHA256",
        hashlib.sha256(manifest.read_bytes()).hexdigest(),
    )
    monkeypatch.setattr(relay_runtime, "QUEUE_CREDENTIAL_FILE", str(credential))
    calls = []
    queue = Queue()

    def factory(**kwargs):
        calls.append(kwargs)
        return queue

    monkeypatch.setattr(relay_runtime, "version", lambda _name: "1.0.0")
    monkeypatch.setattr(
        relay_runtime,
        "import_module",
        lambda _name: SimpleNamespace(create_transport=factory),
    )
    assert _transport() is queue
    assert queue.metadata_calls == 1
    assert calls == [
        {
            "endpoints": {channel: endpoint(channel) for channel in ENDPOINT_ENV},
            "credential_file": str(credential),
            "contract_manifest_file": str(manifest),
            "contract_manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
        }
    ]

    order = []
    monkeypatch.setattr(relay_runtime, "_transport", lambda: order.append("transport") or queue)
    monkeypatch.setattr(
        relay_runtime,
        "build_runtime_service",
        lambda: order.append("service") or SimpleNamespace(repository=object()),
    )
    monkeypatch.setenv("JUNTAI_RELAY_ID", "relay-test")
    relay_runtime.build_runtime_relay()
    assert order == ["transport", "service"]


def test_control_ordering_key_is_stable_opaque_and_fails_closed() -> None:
    key = ordering_key_for(
        CONTROL_CHANNEL,
        message_id="cancel-one",
        tenant_id="tenant-secret",
        job_id="job-secret",
        attempt_id="attempt-secret",
    )
    assert key == ordering_key_for(
        CONTROL_CHANNEL,
        message_id="cancel-replay",
        tenant_id="tenant-secret",
        job_id="job-secret",
        attempt_id="attempt-secret",
    )
    assert key == "ff0710e12c3ecbc9c13992c52c9ce357dc55c2fb13d0fa7a305ca68278dba0ca"
    assert all(value not in key for value in ("tenant-secret", "job-secret", "attempt-secret"))
    assert validate_publish_ordering_key(CONTROL_CHANNEL, key) == key
    with pytest.raises(QueueBindingError, match="absent"):
        validate_publish_ordering_key(CONTROL_CHANNEL, None)
    with pytest.raises(QueueBindingError, match="opaque identity"):
        validate_publish_ordering_key(CONTROL_CHANNEL, "tenant/job/attempt")
    with pytest.raises(QueueBindingError, match="bounded"):
        validate_publish_ordering_key(DISPATCH_CHANNEL, "message id")
    with pytest.raises(QueueBindingError, match="absent or invalid"):
        ordering_key_for(
            CONTROL_CHANNEL,
            message_id="cancel-one",
            tenant_id="tenant-secret",
            job_id="job-secret",
            attempt_id="",
        )


@pytest.mark.parametrize(
    ("channel", "value", "message"),
    [
        (
            DISPATCH_CHANNEL,
            f"kafka+tls://event-queue.juntai-platform.svc.cluster.local:9093/{DISPATCH_CHANNEL}",
            "exact executor QueueTransport endpoint",
        ),
        (DISPATCH_CHANNEL, endpoint(DISPATCH_CHANNEL, address="8.8.8.8"), "Service ClusterIP"),
        (
            DISPATCH_CHANNEL,
            endpoint(DISPATCH_CHANNEL) + "&fallback=kafka",
            "exact executor QueueTransport endpoint",
        ),
        (
            DISPATCH_CHANNEL,
            endpoint(DISPATCH_CHANNEL).replace("serverName=", "other=", 1),
            "exact executor QueueTransport endpoint",
        ),
    ],
)
def test_queue_endpoint_rejects_direct_broker_public_or_extra_components(
    channel: str, value: str, message: str
) -> None:
    from juntai_synthetic_data.relay_runtime import _validate_endpoint

    with pytest.raises(RuntimeError, match=message):
        _validate_endpoint(value, channel=channel, environment_name=ENDPOINT_ENV[channel])


def test_outbox_retry_delay_preserves_the_service_owned_publish_bounds() -> None:
    assert outbox_retry_delay(1, random_value=lambda: 0) == 0
    assert outbox_retry_delay(1, random_value=lambda: 1) == 5
    assert outbox_retry_delay(5, random_value=lambda: 1) == 80


def test_outbox_leases_are_concurrent_safe_and_expire_for_restart_recovery() -> None:
    _, repository, _, _ = queued()
    now = datetime.now(UTC)
    first = repository.lease_outbox(relay_id="relay-a", limit=10, now=now, lease_seconds=60)
    assert len(first) == 1
    assert repository.lease_outbox(relay_id="relay-b", limit=10, now=now, lease_seconds=60) == ()
    recovered = repository.lease_outbox(
        relay_id="relay-b", limit=10, now=now + timedelta(seconds=61), lease_seconds=60
    )
    assert len(recovered) == 1
    assert recovered[0].message_id == first[0].message_id
    assert recovered[0].lease_token != first[0].lease_token


class LoseFirstCommit:
    def __init__(self, repository) -> None:
        self.repository = repository
        self.lost = False

    def __getattr__(self, name):
        return getattr(self.repository, name)

    def mark_outbox_published(self, *args, **kwargs):
        if not self.lost:
            self.lost = True
            return False
        return self.repository.mark_outbox_published(*args, **kwargs)


def test_publish_keeps_exact_bytes_and_replays_after_broker_ack_before_kes_commit() -> None:
    service, repository, _, dispatch = queued()
    queue = Queue()
    relay = SyntheticRelay(
        relay_id="relay-a",
        repository=LoseFirstCommit(repository),
        service=service,
        transport=queue,
        random_value=lambda: 0,
    )
    assert relay.publish_outbox() == 0
    assert relay.publish_outbox() == 1
    assert len(queue.publications) == 2
    assert queue.publications[0] == queue.publications[1]
    assert queue.publications[0][3] == dispatch.canonical_bytes()
    assert queue.publications[0][4] == dispatch.message_id
    assert repository.pending_outbox() == ()


def test_control_publish_passes_exact_service_owned_ordering_key() -> None:
    service, repository, status, _ = queued()
    queue = Queue()
    relay = SyntheticRelay(
        relay_id="relay-a", repository=repository, service=service, transport=queue
    )
    assert relay.publish_outbox() == 1
    service.cancel("tenant-a", status.job_id)
    control = repository.pending_outbox()[0]
    assert relay.publish_outbox() == 1
    publication = next(value for value in queue.publications if value[0] == CONTROL_CHANNEL)
    expected = ordering_key_for(
        CONTROL_CHANNEL,
        message_id=control.message_id,
        tenant_id=control.tenant_id,
        job_id=control.job_id,
        attempt_id=control.attempt_id,
    )
    assert publication[4] == expected
    assert publication[3] == control.canonical_bytes


def test_mismatched_publish_receipt_never_marks_the_kes_outbox() -> None:
    service, repository, _, _ = queued()

    class MismatchedReceiptQueue(Queue):
        def publish(self, channel, *, message_id, content_digest, canonical_bytes, ordering_key):
            super().publish(
                channel,
                message_id=message_id,
                content_digest=content_digest,
                canonical_bytes=canonical_bytes,
                ordering_key=ordering_key,
            )
            return PublicationReceipt(message_id, "sha256:" + "0" * 64, "broker-mismatch")

    queue = MismatchedReceiptQueue()
    relay = SyntheticRelay(
        relay_id="relay-a",
        repository=repository,
        service=service,
        transport=queue,
        random_value=lambda: 0,
    )
    assert relay.publish_outbox() == 0
    assert len(repository.pending_outbox()) == 1


def test_result_is_acknowledged_only_after_transactional_inbox_state_commit() -> None:
    service, repository, status, dispatch = queued()
    started = event(
        dispatch, event_id="relay-started", event_type="STARTED", stage="RUNNING", sequence=0
    )
    queue = Queue()
    queue.deliveries[RESULT_CHANNEL].append(delivery(started))

    def committed_before_ack() -> None:
        assert service.get_job("tenant-a", status.job_id).state is JobState.RUNNING

    queue.before_ack = committed_before_ack
    relay = SyntheticRelay(
        relay_id="relay-a", repository=repository, service=service, transport=queue
    )
    assert relay.consume(RESULT_CHANNEL) == 1
    assert queue.settlements == [("ack", RECEIPT, None)]


class FailingService:
    def accept_worker_event(self, event, *, authenticated_producer):
        raise RuntimeError("KES unavailable")

    def accept_dead_letter(self, record):
        raise RuntimeError("KES unavailable")


def test_queue_or_kes_failure_never_acks_or_claims_service_owned_fifth_authority() -> None:
    _, repository, _, dispatch = queued()
    started = event(
        dispatch, event_id="relay-retry", event_type="STARTED", stage="RUNNING", sequence=0
    )
    queue = Queue()
    queue.deliveries[RESULT_CHANNEL].append(delivery(started, delivery_count=1))
    relay = SyntheticRelay(
        relay_id="relay-a",
        repository=repository,
        service=FailingService(),
        transport=queue,
        random_value=lambda: 0.5,
    )
    relay.consume(RESULT_CHANNEL)
    assert queue.settlements == [("release", RECEIPT, None)]

    queue.settlements.clear()
    queue.deliveries[RESULT_CHANNEL].append(delivery(started, delivery_count=5))
    relay.consume(RESULT_CHANNEL)
    assert queue.settlements == [("release", RECEIPT, None)]


def test_identity_spoof_is_rejected_without_a_job_transition() -> None:
    service, repository, status, dispatch = queued()
    started = event(
        dispatch, event_id="relay-spoof", event_type="STARTED", stage="RUNNING", sequence=0
    )
    queue = Queue()
    queue.deliveries[RESULT_CHANNEL].append(delivery(started, producer=PLATFORM_WORKER))
    relay = SyntheticRelay(
        relay_id="relay-a", repository=repository, service=service, transport=queue
    )
    relay.consume(RESULT_CHANNEL)
    assert queue.settlements == [("reject", RECEIPT, None)]
    assert service.get_job("tenant-a", status.job_id).state is JobState.QUEUED


@pytest.mark.parametrize(
    ("change", "reason"),
    [
        ({"channel": DEAD_LETTER_CHANNEL}, "ENVELOPE_INVALID"),
        ({"receipt": ""}, "ENVELOPE_INVALID"),
        ({"receipt": "not-base64url-bounded"}, "ENVELOPE_INVALID"),
        ({"delivery_count": 0}, "ENVELOPE_INVALID"),
        ({"lease_expires_at": datetime.now(UTC) - timedelta(seconds=1)}, "LEASE_EXPIRED"),
        (
            {
                "lease_expires_at": datetime.now(timezone(timedelta(hours=1)))
                + timedelta(seconds=60)
            },
            "LEASE_EXPIRED",
        ),
        ({"authenticated_producer": None}, "IDENTITY_MISMATCH"),
        ({"original_channel": DISPATCH_CHANNEL}, "ENVELOPE_INVALID"),
    ],
)
def test_invalid_transport_delivery_metadata_fails_closed(change, reason) -> None:
    service, repository, _, dispatch = queued()
    started = event(
        dispatch,
        event_id="relay-invalid-metadata",
        event_type="STARTED",
        stage="RUNNING",
        sequence=0,
    )
    invalid = replace(delivery(started), **change)
    queue = Queue()
    queue.deliveries[RESULT_CHANNEL].append(invalid)
    relay = SyntheticRelay(
        relay_id="relay-a", repository=repository, service=service, transport=queue
    )
    relay.consume(RESULT_CHANNEL)
    assert queue.settlements == [("reject", invalid.receipt, None)]
    assert SyntheticRelay._reason(ProtocolError(reason, "test")) == reason


class SlowService:
    def __init__(self, service) -> None:
        self.service = service

    def accept_worker_event(self, event, *, authenticated_producer):
        time.sleep(0.04)
        return self.service.accept_worker_event(
            event, authenticated_producer=authenticated_producer
        )

    def accept_dead_letter(self, record):
        return self.service.accept_dead_letter(record)


def test_long_result_commit_renews_visibility_before_ack() -> None:
    service, repository, _, dispatch = queued()
    started = event(
        dispatch, event_id="relay-renew", event_type="STARTED", stage="RUNNING", sequence=0
    )
    queue = Queue()
    queue.deliveries[RESULT_CHANNEL].append(delivery(started))
    relay = SyntheticRelay(
        relay_id="relay-a",
        repository=repository,
        service=SlowService(service),
        transport=queue,
        renew_every_seconds=0.01,
    )
    relay.consume(RESULT_CHANNEL)
    assert queue.renewals >= 1
    assert queue.settlements == [("ack", RECEIPT, None)]


@pytest.mark.parametrize("failure_point", ["renew", "acknowledge"])
def test_receipt_lease_loss_never_attempts_a_second_settlement(failure_point: str) -> None:
    service, repository, status, dispatch = queued()
    started = event(
        dispatch,
        event_id=f"relay-{failure_point}-lease-lost",
        event_type="STARTED",
        stage="RUNNING",
        sequence=0,
    )

    class LostReceiptQueue(Queue):
        def renew(self, receipt, *, visibility_seconds):
            if failure_point == "renew":
                raise QueueLeaseLostError("receipt generation is fenced")
            return super().renew(receipt, visibility_seconds=visibility_seconds)

        def acknowledge(self, receipt):
            if failure_point == "acknowledge":
                raise QueueLeaseLostError("receipt generation is fenced")
            return super().acknowledge(receipt)

    queue = LostReceiptQueue()
    queue.deliveries[RESULT_CHANNEL].append(delivery(started))
    relay = SyntheticRelay(
        relay_id="relay-a",
        repository=repository,
        service=SlowService(service) if failure_point == "renew" else service,
        transport=queue,
        renew_every_seconds=0.01,
    )
    with pytest.raises(QueueLeaseLostError, match="fenced"):
        relay.consume(RESULT_CHANNEL)
    assert queue.settlements == []
    assert service.get_job("tenant-a", status.job_id).state is JobState.RUNNING


def test_five_delivery_dead_letter_is_committed_with_terminal_mapping() -> None:
    service, repository, status, dispatch = queued()
    queue = Queue()
    queue.deliveries[DEAD_LETTER_CHANNEL].append(
        ReceivedMessage(
            channel=DEAD_LETTER_CHANNEL,
            message_id=dispatch.message_id,
            content_digest=dispatch.content_digest,
            canonical_bytes=dispatch.canonical_bytes(),
            receipt=DLQ_RECEIPT,
            delivery_count=1,
            lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
            authenticated_producer=PLATFORM_API,
            original_channel=DISPATCH_CHANNEL,
            original_delivery_count=5,
            original_content_digest=dispatch.content_digest,
            terminal_reason_code="DELIVERY_EXHAUSTED",
            ledger_evidence_id="ledger-evidence-dispatch",
        )
    )
    relay = SyntheticRelay(
        relay_id="relay-a", repository=repository, service=service, transport=queue
    )
    relay.consume(DEAD_LETTER_CHANNEL)
    failed = service.get_job("tenant-a", status.job_id)
    assert failed.state is JobState.FAILED
    assert failed.failure and failed.failure["code"] == "DELIVERY_EXHAUSTED"
    assert queue.settlements == [("ack", DLQ_RECEIPT, None)]


@pytest.mark.parametrize(
    "change",
    [
        {"original_delivery_count": 4},
        {"original_content_digest": "sha256:" + "0" * 64},
        {"terminal_reason_code": None},
        {"terminal_reason_code": ""},
        {"terminal_reason_code": "not-generic\n"},
        {"ledger_evidence_id": None},
        {"ledger_evidence_id": ""},
        {"ledger_evidence_id": "ledger-evidence\n"},
    ],
)
def test_dead_letter_missing_platform_evidence_fails_closed(change) -> None:
    service, repository, status, dispatch = queued()
    value = ReceivedMessage(
        channel=DEAD_LETTER_CHANNEL,
        message_id=dispatch.message_id,
        content_digest=dispatch.content_digest,
        canonical_bytes=dispatch.canonical_bytes(),
        receipt=DLQ_RECEIPT,
        delivery_count=1,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        authenticated_producer=PLATFORM_API,
        original_channel=DISPATCH_CHANNEL,
        original_delivery_count=5,
        original_content_digest=dispatch.content_digest,
        terminal_reason_code="DELIVERY_EXHAUSTED",
        ledger_evidence_id="ledger-evidence-dispatch",
    )
    queue = Queue()
    queue.deliveries[DEAD_LETTER_CHANNEL].append(replace(value, **change))
    relay = SyntheticRelay(
        relay_id="relay-a", repository=repository, service=service, transport=queue
    )
    relay.consume(DEAD_LETTER_CHANNEL)
    assert queue.settlements == [("reject", DLQ_RECEIPT, None)]
    assert service.get_job("tenant-a", status.job_id).state is JobState.QUEUED


def test_result_conflict_dead_letter_is_quarantined_without_replacing_job_state() -> None:
    service, repository, status, dispatch = queued()
    first = event(
        dispatch, event_id="relay-conflict", event_type="STARTED", stage="RUNNING", sequence=0
    )
    service.accept_worker_event(first, authenticated_producer=EXECUTOR)
    conflict = first.model_copy(update={"sequence": 1, "content_digest": None}).signed()
    queue = Queue()
    dead_letter = ReceivedMessage(
        channel=DEAD_LETTER_CHANNEL,
        message_id=conflict.message_id,
        content_digest=conflict.content_digest,
        canonical_bytes=conflict.canonical_bytes(),
        receipt=CONFLICT_RECEIPT,
        delivery_count=1,
        lease_expires_at=datetime.now(UTC) + timedelta(seconds=60),
        authenticated_producer=PLATFORM_EXECUTOR,
        original_channel=RESULT_CHANNEL,
        original_delivery_count=5,
        original_content_digest=conflict.content_digest,
        terminal_reason_code="RESULT_CONFLICT",
        ledger_evidence_id="ledger-evidence-result",
    )
    queue.deliveries[DEAD_LETTER_CHANNEL].append(dead_letter)
    relay = SyntheticRelay(
        relay_id="relay-a", repository=repository, service=service, transport=queue
    )
    relay.consume(DEAD_LETTER_CHANNEL)
    assert service.get_job("tenant-a", status.job_id).state is JobState.RUNNING
    assert queue.settlements == [("ack", CONFLICT_RECEIPT, None)]
    queue.settlements.clear()
    queue.deliveries[DEAD_LETTER_CHANNEL].append(
        replace(
            dead_letter,
            receipt=DLQ_RECEIPT,
            ledger_evidence_id="changed-ledger-evidence",
        )
    )
    relay.consume(DEAD_LETTER_CHANNEL)
    assert queue.settlements == [("reject", DLQ_RECEIPT, None)]
