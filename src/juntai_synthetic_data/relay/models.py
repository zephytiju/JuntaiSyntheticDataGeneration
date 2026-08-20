"""Bounded queue and KES relay records for the service-owned SWP bridge."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime

from juntai_synthetic_data.worker_protocol import WorkloadIdentity

QUEUE_BINDING_PROFILE = "juntai.platform.queue-binding/v1"
KAFKA_PRODUCT = "Apache Kafka"
KAFKA_VERSION = "4.1.1"
KAFKA_IMAGE_DIGEST = "sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40"


@dataclass(frozen=True)
class OutboxLease:
    tenant_id: str
    job_id: str
    attempt_id: str
    channel: str
    message_id: str
    content_digest: str
    canonical_bytes: bytes
    sequence: int
    lease_token: str
    lease_expires_at: datetime
    publish_attempts: int


@dataclass(frozen=True)
class DeadLetterRecord:
    dead_letter_id: str
    tenant_id: str
    job_id: str
    attempt_id: str
    original_channel: str
    message_id: str
    content_digest: str
    original_content_digest: str
    canonical_bytes: bytes
    delivery_count: int
    authenticated_producer: WorkloadIdentity | None
    terminal_reason_code: str
    ledger_evidence_id: str
    event_id: str | None = None


def dead_letter_record_digest(record: DeadLetterRecord) -> str:
    """Bind every authenticated DLQ evidence field for inbox replay/conflict checks."""

    producer = record.authenticated_producer
    components = (
        record.tenant_id,
        record.job_id,
        record.attempt_id,
        record.original_channel,
        record.message_id,
        record.content_digest,
        record.original_content_digest,
        "sha256:" + hashlib.sha256(record.canonical_bytes).hexdigest(),
        str(record.delivery_count),
        producer.namespace if producer is not None else "",
        producer.service_account if producer is not None else "",
        record.terminal_reason_code,
        record.ledger_evidence_id,
        record.event_id or "",
    )
    material = b"".join(
        len(value.encode("utf-8")).to_bytes(4, "big") + value.encode("utf-8")
        for value in components
    )
    return "sha256:" + hashlib.sha256(material).hexdigest()
