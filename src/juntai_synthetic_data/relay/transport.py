"""Queue-neutral Platform capability contract used only by the API-side relay."""

from __future__ import annotations

import hashlib

from juntai_platform_queue_kafka import QueueBindingMetadata

from juntai_synthetic_data.execution.coordinator import (
    CONTROL_CHANNEL,
    DEAD_LETTER_CHANNEL,
    DISPATCH_CHANNEL,
    RESULT_CHANNEL,
)
from juntai_synthetic_data.worker_protocol import (
    MAXIMUM_DELIVERIES,
    RENEW_EVERY_SECONDS,
    VISIBILITY_SECONDS,
)

from .models import (
    KAFKA_IMAGE_DIGEST,
    KAFKA_PRODUCT,
    KAFKA_VERSION,
    QUEUE_BINDING_PROFILE,
)

REQUIRED_CHANNELS = (
    DISPATCH_CHANNEL,
    CONTROL_CHANNEL,
    RESULT_CHANNEL,
    DEAD_LETTER_CHANNEL,
)
CONTROL_ORDERING_DOMAIN = b"juntai.synthetic.control-order/v1\x00"
MAX_ORDERING_KEY_BYTES = 256


class QueueBindingError(RuntimeError):
    """The injected Platform capability cannot prove the required SWP semantics."""


def validate_binding_metadata(metadata: QueueBindingMetadata | None) -> None:
    if metadata is None:
        raise QueueBindingError("Platform queue capability metadata is absent")
    expected = QueueBindingMetadata(
        schema_version=QUEUE_BINDING_PROFILE,
        channels=REQUIRED_CHANNELS,
        provider=KAFKA_PRODUCT,
        provider_version=KAFKA_VERSION,
        provider_image_digest=KAFKA_IMAGE_DIGEST,
        delivery_semantics="at-least-once-platform-ledger-authoritative",
        visibility_seconds=VISIBILITY_SECONDS,
        renew_every_seconds=RENEW_EVERY_SECONDS,
        maximum_deliveries=MAXIMUM_DELIVERIES,
        idempotent_publish=True,
        dead_letter=True,
    )
    if metadata != expected:
        raise QueueBindingError("Platform queue capability metadata is incompatible with SWP/v1")


def ordering_key_for(
    channel: str,
    *,
    message_id: str,
    tenant_id: str,
    job_id: str,
    attempt_id: str,
) -> str:
    """Return the stable opaque service-owned ordering key for one publication."""

    if channel not in REQUIRED_CHANNELS:
        raise QueueBindingError("publication channel is not part of the SWP/v1 binding")
    if not message_id:
        raise QueueBindingError("publication message identity is absent")
    if channel != CONTROL_CHANNEL:
        return message_id
    components = (tenant_id, job_id, attempt_id)
    if any(not value or len(value.encode("utf-8")) > 1024 for value in components):
        raise QueueBindingError("control ordering identity is absent or invalid")
    material = CONTROL_ORDERING_DOMAIN + b"\x00".join(value.encode("utf-8") for value in components)
    return hashlib.sha256(material).hexdigest()


def validate_publish_ordering_key(channel: str, ordering_key: str | None) -> str:
    if not isinstance(ordering_key, str) or not ordering_key:
        raise QueueBindingError("publication ordering key is absent")
    encoded = ordering_key.encode("utf-8")
    if len(encoded) > MAX_ORDERING_KEY_BYTES or any(
        character.isspace() or ord(character) < 32 or ord(character) == 127
        for character in ordering_key
    ):
        raise QueueBindingError("publication ordering key is not bounded")
    if channel == CONTROL_CHANNEL and not (
        len(ordering_key) == 64
        and all(character in "0123456789abcdef" for character in ordering_key)
    ):
        raise QueueBindingError("control ordering key is not a Synthetic opaque identity")
    return ordering_key
