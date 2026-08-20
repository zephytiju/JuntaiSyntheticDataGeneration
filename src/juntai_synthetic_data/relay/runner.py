"""Crash-safe API-side outbox publisher and authenticated result/DLQ consumer."""

from __future__ import annotations

import hashlib
import random
import re
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Protocol

from juntai_platform_queue_kafka import (
    AuthenticatedProducer,
    QueueAuthenticationError,
    QueueAuthorizationError,
    QueueConfigurationError,
    QueueConflictError,
    QueueContractError,
    QueueLeaseLostError,
    QueueReceiptError,
    QueueTimeoutError,
    QueueTransport,
    QueueTransportError,
    QueueUnavailableError,
    ReceivedMessage,
)

from juntai_synthetic_data.errors import ErrorCode, SyntheticDataError
from juntai_synthetic_data.execution.coordinator import (
    CONTROL_CHANNEL,
    DEAD_LETTER_CHANNEL,
    DISPATCH_CHANNEL,
    RESULT_CHANNEL,
)
from juntai_synthetic_data.relay.models import DeadLetterRecord, OutboxLease
from juntai_synthetic_data.relay.transport import (
    ordering_key_for,
    validate_binding_metadata,
    validate_publish_ordering_key,
)
from juntai_synthetic_data.worker_protocol import (
    MAX_LEASE_SECONDS,
    MAXIMUM_DELIVERIES,
    RENEW_EVERY_SECONDS,
    TERMINATION_ALLOWANCE_SECONDS,
    VISIBILITY_SECONDS,
    CancelEnvelope,
    DispatchEnvelope,
    ProtocolError,
    WorkerEventEnvelope,
    WorkloadIdentity,
    decode_envelope,
    retry_delay_upper_bound,
)


class RelayRepository(Protocol):
    def lease_outbox(
        self,
        *,
        relay_id: str,
        limit: int,
        now: datetime,
        lease_seconds: int,
    ) -> tuple[OutboxLease, ...]: ...

    def mark_outbox_published(
        self,
        message_id: str,
        lease_token: str,
        *,
        publication_id: str,
        published_at: datetime,
    ) -> bool: ...

    def release_outbox_lease(
        self,
        message_id: str,
        lease_token: str,
        *,
        next_attempt_at: datetime,
        error_code: str,
    ) -> bool: ...


class RelayService(Protocol):
    def accept_worker_event(self, event, *, authenticated_producer) -> str: ...

    def accept_dead_letter(self, record: DeadLetterRecord) -> str: ...


@dataclass(frozen=True)
class RelayRun:
    published: int
    results: int
    dead_letters: int


_OPAQUE_RECEIPT = re.compile(r"^[A-Za-z0-9_-]{43,2048}$")
_PLATFORM_ASCII = re.compile(r"^[\x20-\x7e]{1,512}$")
_REASON_CODE = re.compile(r"^[\x20-\x7e]{1,128}$")
_PRODUCER_COMPONENT = re.compile(r"^.{1,253}$", re.DOTALL)
_PLATFORM_PERMANENT_ERRORS = (
    QueueConfigurationError,
    QueueContractError,
    QueueAuthenticationError,
    QueueAuthorizationError,
    QueueConflictError,
    QueueReceiptError,
    QueueLeaseLostError,
)
_PLATFORM_RETRYABLE_ERRORS = (QueueUnavailableError, QueueTimeoutError)


def outbox_retry_delay(publish_attempt: int, *, random_value: Callable[[], float]) -> float:
    """Bound only Synthetic's KES outbox publication retry schedule."""

    upper = retry_delay_upper_bound(publish_attempt)
    value = random_value()
    if not 0 <= value <= 1:
        raise ValueError("random source must return a value in [0, 1]")
    return value * upper


class _VisibilityHeartbeat:
    def __init__(
        self,
        transport: QueueTransport,
        delivery: ReceivedMessage,
        *,
        renew_until: datetime,
        now: Callable[[], datetime],
        renew_every_seconds: float,
    ) -> None:
        self.transport = transport
        self.delivery = delivery
        self.renew_until = renew_until
        self.now = now
        self.renew_every_seconds = renew_every_seconds
        self.stop = threading.Event()
        self.error: BaseException | None = None
        self.thread = threading.Thread(target=self._run, name="swp-visibility-renewal", daemon=True)

    def __enter__(self) -> _VisibilityHeartbeat:
        self.thread.start()
        return self

    def _run(self) -> None:
        while not self.stop.wait(self.renew_every_seconds):
            remaining = int((self.renew_until - self.now()).total_seconds())
            if remaining <= 0:
                self.error = RuntimeError("SWP visibility renewal reached its bounded deadline")
                return
            try:
                self.transport.renew(
                    self.delivery.receipt,
                    visibility_seconds=min(VISIBILITY_SECONDS, remaining),
                )
            except BaseException as error:  # retained for the processing thread
                self.error = error
                return

    def __exit__(self, *_args: object) -> None:
        self.stop.set()
        self.thread.join(timeout=max(1.0, self.renew_every_seconds * 2))


class SyntheticRelay:
    def __init__(
        self,
        *,
        relay_id: str,
        repository: RelayRepository,
        service: RelayService,
        transport: QueueTransport,
        batch_size: int = 25,
        now: Callable[[], datetime] | None = None,
        random_value: Callable[[], float] | None = None,
        renew_every_seconds: float = RENEW_EVERY_SECONDS,
    ) -> None:
        if not relay_id or len(relay_id) > 128:
            raise ValueError("relay identity must be a bounded non-empty string")
        if not 1 <= batch_size <= 100:
            raise ValueError("relay batch size must be in [1, 100]")
        if not 0 < renew_every_seconds <= RENEW_EVERY_SECONDS:
            raise ValueError("visibility renewal period exceeds SWP/v1")
        self.relay_id = relay_id
        self.repository = repository
        self.service = service
        self.transport = transport
        self.batch_size = batch_size
        self.now = now or (lambda: datetime.now(UTC))
        self.random_value = random_value or random.random
        self.renew_every_seconds = renew_every_seconds

    @staticmethod
    def _outbox_envelope(lease: OutboxLease) -> DispatchEnvelope | CancelEnvelope:
        envelope = decode_envelope(lease.canonical_bytes)
        expected_type = DispatchEnvelope if lease.channel == DISPATCH_CHANNEL else CancelEnvelope
        if lease.channel not in {DISPATCH_CHANNEL, CONTROL_CHANNEL} or not isinstance(
            envelope, expected_type
        ):
            raise ProtocolError("ENVELOPE_INVALID", "outbox channel and envelope kind differ")
        if (
            envelope.message_id != lease.message_id
            or envelope.content_digest != lease.content_digest
            or envelope.tenant_id != lease.tenant_id
            or envelope.job_id != lease.job_id
            or envelope.attempt_id != lease.attempt_id
            or envelope.sequence != lease.sequence
        ):
            raise ProtocolError("ENVELOPE_DIGEST_MISMATCH", "outbox metadata differs from bytes")
        return envelope

    def publish_outbox(self) -> int:
        now = self.now()
        leases = self.repository.lease_outbox(
            relay_id=self.relay_id,
            limit=self.batch_size,
            now=now,
            lease_seconds=VISIBILITY_SECONDS,
        )
        published = 0
        for lease in leases:
            try:
                self._outbox_envelope(lease)
                ordering_key = validate_publish_ordering_key(
                    lease.channel,
                    ordering_key_for(
                        lease.channel,
                        message_id=lease.message_id,
                        tenant_id=lease.tenant_id,
                        job_id=lease.job_id,
                        attempt_id=lease.attempt_id,
                    ),
                )
                receipt = self.transport.publish(
                    lease.channel,
                    message_id=lease.message_id,
                    content_digest=lease.content_digest,
                    canonical_bytes=lease.canonical_bytes,
                    ordering_key=ordering_key,
                )
                if (
                    receipt.message_id != lease.message_id
                    or receipt.content_digest != lease.content_digest
                    or not isinstance(receipt.publication_id, str)
                    or not _PLATFORM_ASCII.fullmatch(receipt.publication_id)
                ):
                    raise ProtocolError(
                        "ENVELOPE_DIGEST_MISMATCH",
                        "transport publication receipt differs from the KES outbox identity",
                    )
                if not self.repository.mark_outbox_published(
                    lease.message_id,
                    lease.lease_token,
                    publication_id=receipt.publication_id,
                    published_at=self.now(),
                ):
                    raise RuntimeError("outbox publication lease was lost before KES commit")
                published += 1
            except Exception as error:
                count = min(lease.publish_attempts, MAXIMUM_DELIVERIES)
                delay = outbox_retry_delay(count, random_value=self.random_value)
                self.repository.release_outbox_lease(
                    lease.message_id,
                    lease.lease_token,
                    next_attempt_at=self.now() + timedelta(seconds=delay),
                    error_code=self._reason(error),
                )
        return published

    @staticmethod
    def _reason(error: BaseException) -> str:
        if isinstance(error, QueueTransportError):
            return error.code
        if isinstance(error, ProtocolError):
            return error.code
        if isinstance(error, SyntheticDataError):
            return str(error.details.get("protocol_error", error.code.value))[:64]
        return "DEPENDENCY_UNAVAILABLE"

    @staticmethod
    def _is_permanent(error: BaseException) -> bool:
        if isinstance(error, _PLATFORM_PERMANENT_ERRORS):
            return True
        if isinstance(error, _PLATFORM_RETRYABLE_ERRORS):
            return False
        if isinstance(error, ProtocolError):
            return True
        if not isinstance(error, SyntheticDataError):
            return False
        if error.code in {ErrorCode.POLICY_DENIED, ErrorCode.CONTRACT_INVALID}:
            return True
        return error.details.get("protocol_error") == "RESULT_CONFLICT"

    @staticmethod
    def _producer_identity(producer: AuthenticatedProducer) -> WorkloadIdentity:
        if (
            not isinstance(producer, AuthenticatedProducer)
            or not _PRODUCER_COMPONENT.fullmatch(producer.namespace)
            or not _PRODUCER_COMPONENT.fullmatch(producer.service_account)
        ):
            raise ProtocolError("IDENTITY_MISMATCH", "transport producer identity is invalid")
        return WorkloadIdentity(
            namespace=producer.namespace,
            serviceAccount=producer.service_account,
        )

    def _validate_delivery(self, delivery: ReceivedMessage, *, expected_channel: str):
        if delivery.channel != expected_channel or expected_channel not in {
            RESULT_CHANNEL,
            DEAD_LETTER_CHANNEL,
        }:
            raise ProtocolError(
                "ENVELOPE_INVALID", "transport returned a delivery on the wrong channel"
            )
        if not isinstance(delivery.receipt, str) or not _OPAQUE_RECEIPT.fullmatch(delivery.receipt):
            raise ProtocolError("ENVELOPE_INVALID", "transport delivery receipt is not bounded")
        if not 1 <= delivery.delivery_count <= MAXIMUM_DELIVERIES:
            raise ProtocolError("ENVELOPE_INVALID", "transport delivery count is invalid")
        if (
            not isinstance(delivery.lease_expires_at, datetime)
            or delivery.lease_expires_at.utcoffset() != timedelta(0)
            or delivery.lease_expires_at <= self.now()
        ):
            raise ProtocolError("LEASE_EXPIRED", "transport delivery lease is expired")
        if delivery.authenticated_producer is None:
            raise ProtocolError("IDENTITY_MISMATCH", "transport producer is unauthenticated")
        self._producer_identity(delivery.authenticated_producer)
        evidence = (
            delivery.original_channel,
            delivery.original_delivery_count,
            delivery.original_content_digest,
            delivery.terminal_reason_code,
            delivery.ledger_evidence_id,
        )
        if expected_channel != DEAD_LETTER_CHANNEL and any(value is not None for value in evidence):
            raise ProtocolError(
                "ENVELOPE_INVALID", "non-dead-letter delivery exposes dead-letter evidence"
            )
        if not isinstance(delivery.canonical_bytes, bytes) or not (
            1 <= len(delivery.canonical_bytes) <= 1_048_576
        ):
            raise ProtocolError("ENVELOPE_INVALID", "transport canonical bytes are not bounded")
        envelope = decode_envelope(delivery.canonical_bytes)
        if (
            envelope.message_id != delivery.message_id
            or envelope.content_digest != delivery.content_digest
        ):
            raise ProtocolError(
                "ENVELOPE_DIGEST_MISMATCH", "queue metadata differs from canonical envelope"
            )
        return envelope

    @staticmethod
    def _dead_letter(delivery: ReceivedMessage, envelope) -> DeadLetterRecord:
        original_channel = delivery.original_channel
        if original_channel not in {DISPATCH_CHANNEL, CONTROL_CHANNEL, RESULT_CHANNEL}:
            raise ProtocolError("ENVELOPE_INVALID", "dead-letter original channel is invalid")
        expected = {
            DISPATCH_CHANNEL: DispatchEnvelope,
            CONTROL_CHANNEL: CancelEnvelope,
            RESULT_CHANNEL: WorkerEventEnvelope,
        }[original_channel]
        if not isinstance(envelope, expected):
            raise ProtocolError("ENVELOPE_INVALID", "dead-letter channel and envelope kind differ")
        if delivery.original_delivery_count != MAXIMUM_DELIVERIES:
            raise ProtocolError("ENVELOPE_INVALID", "dead-letter delivery count is not five")
        if delivery.original_content_digest != delivery.content_digest:
            raise ProtocolError(
                "ENVELOPE_DIGEST_MISMATCH",
                "dead-letter original digest differs from canonical bytes",
            )
        if (
            not isinstance(delivery.terminal_reason_code, str)
            or not _REASON_CODE.fullmatch(delivery.terminal_reason_code)
            or not isinstance(delivery.ledger_evidence_id, str)
            or not _PLATFORM_ASCII.fullmatch(delivery.ledger_evidence_id)
        ):
            raise ProtocolError("ENVELOPE_INVALID", "dead-letter evidence metadata is incomplete")
        if delivery.authenticated_producer is None:
            raise ProtocolError("IDENTITY_MISMATCH", "dead-letter producer is unauthenticated")
        producer = SyntheticRelay._producer_identity(delivery.authenticated_producer)
        envelope.verify(authenticated_producer=producer)
        identity = "\0".join((original_channel, delivery.message_id)).encode()
        return DeadLetterRecord(
            dead_letter_id="dlq_" + hashlib.sha256(identity).hexdigest(),
            tenant_id=envelope.tenant_id,
            job_id=envelope.job_id,
            attempt_id=envelope.attempt_id,
            original_channel=original_channel,
            message_id=envelope.message_id,
            content_digest=delivery.content_digest,
            original_content_digest=delivery.original_content_digest,
            canonical_bytes=delivery.canonical_bytes,
            delivery_count=delivery.original_delivery_count,
            authenticated_producer=producer,
            terminal_reason_code=delivery.terminal_reason_code,
            ledger_evidence_id=delivery.ledger_evidence_id,
            event_id=envelope.event_id if isinstance(envelope, WorkerEventEnvelope) else None,
        )

    def _renew_until(self, envelope) -> datetime:
        return min(
            envelope.deadline + timedelta(seconds=TERMINATION_ALLOWANCE_SECONDS),
            envelope.emitted_at + timedelta(seconds=MAX_LEASE_SECONDS),
        )

    def _retry_delivery(self, delivery: ReceivedMessage, envelope, error: BaseException) -> None:
        reason = self._reason(error)
        if not 1 <= delivery.delivery_count <= MAXIMUM_DELIVERIES or self._is_permanent(error):
            self.transport.reject(delivery.receipt, reason_code=reason)
            return
        remaining = (envelope.deadline - self.now()).total_seconds()
        if remaining <= 0:
            self.transport.reject(delivery.receipt, reason_code="DEADLINE_EXCEEDED")
            return
        self.transport.release(delivery.receipt)

    def _process(self, delivery: ReceivedMessage, *, expected_channel: str) -> None:
        envelope = None
        try:
            envelope = self._validate_delivery(delivery, expected_channel=expected_channel)
            renew_until = self._renew_until(envelope)
            if self.now() >= renew_until:
                raise SyntheticDataError(
                    ErrorCode.DEPENDENCY_DEADLINE,
                    "SWP delivery lease is beyond its bounded deadline",
                    details={"protocol_error": "LEASE_EXPIRED"},
                )
            with _VisibilityHeartbeat(
                self.transport,
                delivery,
                renew_until=renew_until,
                now=self.now,
                renew_every_seconds=self.renew_every_seconds,
            ) as heartbeat:
                if delivery.channel == RESULT_CHANNEL:
                    if not isinstance(envelope, WorkerEventEnvelope):
                        raise ProtocolError(
                            "ENVELOPE_INVALID", "result channel requires WorkerEventEnvelope"
                        )
                    if delivery.authenticated_producer is None:
                        raise SyntheticDataError(
                            ErrorCode.POLICY_DENIED,
                            "result delivery lacks an authenticated producer",
                            details={"protocol_error": "IDENTITY_MISMATCH"},
                        )
                    self.service.accept_worker_event(
                        envelope,
                        authenticated_producer=self._producer_identity(
                            delivery.authenticated_producer
                        ),
                    )
                elif delivery.channel == DEAD_LETTER_CHANNEL:
                    self.service.accept_dead_letter(self._dead_letter(delivery, envelope))
                else:
                    raise ProtocolError("ENVELOPE_INVALID", "relay consumed a forbidden channel")
            if heartbeat.error is not None:
                raise heartbeat.error
            self.transport.acknowledge(delivery.receipt)
        except QueueTransportError:
            raise
        except Exception as error:
            if envelope is None:
                try:
                    envelope = decode_envelope(delivery.canonical_bytes)
                except ProtocolError:
                    self.transport.reject(delivery.receipt, reason_code=self._reason(error))
                    return
            self._retry_delivery(delivery, envelope, error)

    def consume(self, channel: str) -> int:
        deliveries = self.transport.receive(
            channel,
            limit=self.batch_size,
            visibility_seconds=VISIBILITY_SECONDS,
        )
        if len(deliveries) > self.batch_size:
            raise ProtocolError("ENVELOPE_INVALID", "transport returned an oversized batch")
        for delivery in deliveries:
            self._process(delivery, expected_channel=channel)
        return len(deliveries)

    def run_once(self) -> RelayRun:
        return RelayRun(
            published=self.publish_outbox(),
            results=self.consume(RESULT_CHANNEL),
            dead_letters=self.consume(DEAD_LETTER_CHANNEL),
        )

    def run_forever(self, stop: threading.Event, *, poll_seconds: float = 1.0) -> None:
        if not 0.05 <= poll_seconds <= 30:
            raise ValueError("relay poll interval must be in [0.05, 30] seconds")
        validate_binding_metadata(self.transport.binding_metadata())
        while not stop.is_set():
            started = monotonic()
            self.run_once()
            remaining = poll_seconds - (monotonic() - started)
            if remaining > 0:
                stop.wait(remaining)
