from __future__ import annotations

import json
import struct
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from juntai_synthetic_data.platform_adapter_contract import (
    PLATFORM_ADAPTER_CONTRACT_SHA256,
    PLATFORM_ADAPTER_SOURCE_COMMIT,
    PLATFORM_ADAPTER_SOURCE_TREE,
    PLATFORM_QUEUE_WHEEL_SHA256,
    PLATFORM_STREAM_WHEEL_SHA256,
)
from juntai_synthetic_data.worker_protocol import (
    INPUT_MEDIA_TYPE,
    MAX_FRAME_BYTES,
    DispatchEnvelope,
    ExactArtifactReference,
    ProtocolError,
    ResourceEnvelope,
    WorkloadIdentity,
    canonical_json,
    decode_envelope,
    encode_frame,
    retry_delay_upper_bound,
)

ROOT = Path(__file__).parents[2]


def dispatch() -> DispatchEnvelope:
    now = datetime(2026, 8, 19, 12, 0, 0, tzinfo=UTC)
    return DispatchEnvelope(
        messageId="dispatch-golden",
        tenantId="tenant-a",
        jobId="job-golden",
        attemptId="attempt-golden",
        attemptNumber=1,
        sequence=0,
        emittedAt=now,
        deadline=now + timedelta(minutes=5),
        correlationId="correlation-golden",
        traceparent="00-0123456789abcdef0123456789abcdef-0123456789abcdef-01",
        producerWorkload=WorkloadIdentity(namespace="juntai", serviceAccount="synthetic-api"),
        requestDigest="sha256:" + "1" * 64,
        inputArtifact=ExactArtifactReference(
            tenantId="tenant-a",
            artifactId="art-input",
            versionId="artv-input-1",
            manifestDigest="sha256:" + "2" * 64,
            mediaType=INPUT_MEDIA_TYPE,
            byteLength=1024,
            producerBuildId="a" * 40,
        ),
        providerId="juntai.deterministic-tabular",
        providerVersion="1.0.0",
        workerImageDigest="sha256:" + "3" * 64,
        requiredCapabilities=("canonical-envelope-digest",),
        minExecutorBinding="juntai.platform.synthetic-executor/v1",
        resourceEnvelope=ResourceEnvelope(
            cpuMillis=1000, memoryBytes=536870912, ephemeralBytes=1048576, processLimit=64
        ),
        idempotencyKeyDigest="sha256:" + "4" * 64,
    ).signed()


def test_golden_envelope_is_canonical_digest_bound_and_framed() -> None:
    envelope = dispatch()
    payload = envelope.canonical_bytes()
    assert payload == canonical_json(json.loads(payload))
    assert decode_envelope(payload) == envelope
    frame = encode_frame(envelope)
    assert struct.unpack(">I", frame[:4]) == (len(payload),)
    assert frame[4:] == payload


def test_digest_mismatch_duplicate_member_unknown_major_and_oversize_fail_closed() -> None:
    value = json.loads(dispatch().canonical_bytes())
    value["sequence"] = 2
    with pytest.raises(ProtocolError, match="contentDigest"):
        decode_envelope(canonical_json(value))
    with pytest.raises(ProtocolError, match="duplicate"):
        decode_envelope(b'{"kind":"dispatch","kind":"cancel"}')
    value = json.loads(dispatch().canonical_bytes())
    value["kind"] = "dispatch-v2"
    with pytest.raises(ProtocolError) as captured:
        decode_envelope(canonical_json(value))
    assert captured.value.code == "PROTOCOL_UNSUPPORTED"
    with pytest.raises(ProtocolError, match="maximum"):
        decode_envelope(b"{" + b" " * MAX_FRAME_BYTES + b"}")


def test_retry_bounds_are_exact_full_jitter_upper_limits() -> None:
    assert [retry_delay_upper_bound(value) for value in range(1, 6)] == [5, 10, 20, 40, 80]
    with pytest.raises(ValueError):
        retry_delay_upper_bound(6)


def test_rfc8785_orders_names_by_utf16_and_rejects_non_interoperable_numbers() -> None:
    assert canonical_json({"\ue000": 1, "\U00010000": 2}) == (
        '{"\U00010000":2,"\ue000":1}'.encode()
    )
    with pytest.raises(ProtocolError, match="interoperable JSON range"):
        canonical_json({"value": 2**53})
    with pytest.raises(ProtocolError, match="floating-point"):
        canonical_json({"value": 1.5})


def test_committed_schema_and_checksum_are_exact() -> None:
    schema = ROOT / "contracts" / "worker-protocol" / "swp.v1.schema.json"
    checksum = (ROOT / "contracts" / "worker-protocol" / "swp.v1.sha256").read_text()
    import hashlib

    assert hashlib.sha256(schema.read_bytes()).hexdigest() in checksum
    document = json.loads(schema.read_text())
    assert document["x-juntai-framing"]["maximumFrameBytes"] == MAX_FRAME_BYTES
    assert document["x-juntai-production-worker-client"]["factory"] == (
        "juntai_platform_swp_stream:create_worker_client"
    )
    assert document["x-juntai-production-worker-client"]["environment"] == [
        "JUNTAI_SWP_TRANSPORT_FACTORY",
        "JUNTAI_SWP_EXECUTOR_ADDRESS",
        "JUNTAI_SWP_EXECUTOR_CA_FILE",
        "JUNTAI_SWP_WORKLOAD_TOKEN_FILE",
        "JUNTAI_SWP_CLAIM_ID",
        "JUNTAI_SWP_CLAIM_GENERATION",
        "JUNTAI_SWP_POD_UID",
        "JUNTAI_SWP_CONTRACT_MANIFEST_FILE",
        "JUNTAI_SWP_CONTRACT_MANIFEST_SHA256",
    ]
    assert document["x-juntai-production-worker-client"]["externalContract"] == {
        "manifestSha256": PLATFORM_ADAPTER_CONTRACT_SHA256,
        "sourceCommit": PLATFORM_ADAPTER_SOURCE_COMMIT,
        "sourceTree": PLATFORM_ADAPTER_SOURCE_TREE,
        "wheelSha256": PLATFORM_STREAM_WHEEL_SHA256,
    }
    assert document["x-juntai-production-worker-client"]["resultAcceptance"] == {
        "returnMeaning": "durable Platform result-outbox commit accepted",
        "ambiguousFailureReplay": (
            "fresh authenticated session; identical sequence, framedBytes, terminal"
        ),
        "terminalOrdering": "ResultAccepted before close",
    }


def test_transport_spi_is_exact_transport_neutral_and_adapter_bound() -> None:
    path = ROOT / "contracts" / "worker-protocol" / "queue-capability.v1.json"
    checksum = (ROOT / "contracts" / "worker-protocol" / "queue-capability.v1.sha256").read_text()
    import hashlib

    assert hashlib.sha256(path.read_bytes()).hexdigest() in checksum
    contract = json.loads(path.read_text())
    assert contract["configuration"]["adapterStatus"] == "approved-platform-binding"
    assert contract["configuration"]["adapterDistribution"] == (
        "juntai-platform-queue-kafka==1.0.0"
    )
    assert contract["configuration"]["adapterFactory"] == (
        "juntai_platform_queue_kafka:create_transport"
    )
    assert contract["requiredMetadata"]["providerVersion"] == "4.1.1"
    assert contract["requiredMetadata"]["deliverySemantics"] == (
        "at-least-once-platform-ledger-authoritative"
    )
    assert contract["requiredMetadata"]["providerImageDigest"] == (
        "sha256:0bc1bb2478f45b6cea78864df86acdc11e8df2c5172477819a4d12942cbe5d40"
    )
    assert contract["requiredMetadata"]["channels"] == [
        "synthetic.worker.dispatch.v1",
        "synthetic.worker.control.v1",
        "synthetic.worker.result.v1",
        "synthetic.worker.dead-letter.v1",
    ]
    assert "orderingKey" in contract["operations"]["publish"]
    assert "publicationId" in contract["operations"]["publish"]
    assert "delay" not in contract["operations"]["release"].lower()
    assert "Platform computes" in contract["operations"]["release"]
    assert "renew" in contract["operations"]
    assert any("delivery accounting" in item for item in contract["prohibited"])
    assert contract["deliveryMetadata"]["deadLetterAdditional"] == [
        "originalChannel",
        "originalDeliveryCount",
        "originalContentDigest",
        "terminalReasonCode",
        "ledgerEvidenceId",
    ]
    assert len(contract["requiredConformanceEvidence"]) == 7
    assert contract["orderingKey"]["nonControl"] == "messageId"
    assert contract["orderingKey"]["utf8Bytes"] == {"minimum": 1, "maximum": 256}
    assert contract["configuration"]["contractManifestFileEnvironment"] == (
        "JUNTAI_QUEUE_CONTRACT_MANIFEST_FILE"
    )
    assert contract["configuration"]["endpointForm"] == (
        "grpcs://[literal-executor-Service-ClusterIP]:7444/<logical-channel>"
        "?serverName=swp-executor.juntai-platform.svc.cluster.local"
    )
    assert contract["configuration"]["credentialServerName"] == (
        "swp-executor.juntai-platform.svc.cluster.local"
    )
    assert contract["configuration"]["externalContract"] == {
        "manifestSha256": PLATFORM_ADAPTER_CONTRACT_SHA256,
        "sourceCommit": PLATFORM_ADAPTER_SOURCE_COMMIT,
        "sourceTree": PLATFORM_ADAPTER_SOURCE_TREE,
        "wheelSha256": PLATFORM_QUEUE_WHEEL_SHA256,
    }
    assert contract["configuration"]["contractManifestRequired"] == {
        "queueProtoPath": "queue/juntai.platform.queue.v1.proto",
        "queueDescriptorPath": "queue/descriptor.pb",
        "queueRpcService": "juntai.platform.queue.v1.QueueTransport",
        "queueRpcPort": 7444,
        "queueRpcTransport": "tls1.3-h2-mtls",
        "queueRpcResource": "urn:juntai:platform:queue-transport",
        "queueRpcAudience": "juntai-platform-queue-transport",
        "deliveryAuthority": "platform_worker_delivery_v1",
    }
    assert "before any Synthetic KES lease" in contract["configuration"]["remoteReadiness"]
    platform_surface = json.dumps(
        {
            "configuration": contract["configuration"],
            "metadata": contract["requiredMetadata"],
            "operations": contract["operations"],
        },
        sort_keys=True,
    )
    assert all(name not in platform_surface for name in ("tenantId", "jobId", "attemptId"))
    assert "opaque ordering-key generation" in contract["ownership"]["service"]
