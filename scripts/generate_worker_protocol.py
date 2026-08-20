"""Generate deterministic SWP/v1 schema, checksum, and optional exact release manifest."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from pathlib import Path

from pydantic import TypeAdapter

from juntai_synthetic_data import __version__
from juntai_synthetic_data.platform_adapter_contract import (
    PLATFORM_ADAPTER_CONTRACT_SHA256,
    PLATFORM_ADAPTER_SOURCE_COMMIT,
    PLATFORM_ADAPTER_SOURCE_TREE,
    PLATFORM_QUEUE_WHEEL_SHA256,
    PLATFORM_STREAM_WHEEL_SHA256,
)
from juntai_synthetic_data.relay.models import (
    KAFKA_IMAGE_DIGEST,
    KAFKA_PRODUCT,
    KAFKA_VERSION,
    QUEUE_BINDING_PROFILE,
)
from juntai_synthetic_data.relay.transport import REQUIRED_CHANNELS
from juntai_synthetic_data.relay_runtime import (
    QUEUE_CREDENTIAL_FILE,
    QUEUE_CREDENTIAL_SCHEMA,
    QUEUE_SERVER_NAME,
    TRANSPORT_DISTRIBUTION,
    TRANSPORT_FACTORY,
    TRANSPORT_VERSION,
)
from juntai_synthetic_data.worker_protocol import (
    EVIDENCE_MEDIA_TYPE,
    INPUT_MEDIA_TYPE,
    MAX_FRAME_BYTES,
    PROTOCOL_VERSION,
    SOCKET_PATH,
)
from juntai_synthetic_data.worker_protocol.models import Envelope

ROOT = Path(__file__).parents[1]
DEFAULT_OUT = ROOT / "contracts" / "worker-protocol"
_COMMIT = re.compile(r"^[0-9a-f]{40}$")
_DIGEST = re.compile(r"^sha256:[0-9a-f]{64}$")


def _canonical(value: object) -> bytes:
    return (
        json.dumps(
            value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
        )
        + "\n"
    ).encode()


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default=str(DEFAULT_OUT))
    parser.add_argument("--source-commit")
    parser.add_argument("--image-digest")
    args = parser.parse_args()
    if (args.source_commit is None) != (args.image_digest is None):
        raise SystemExit("source commit and image digest must be supplied together")
    if args.source_commit is not None and not _COMMIT.fullmatch(args.source_commit):
        raise SystemExit("source commit must be 40 lowercase hexadecimal characters")
    if args.image_digest is not None and not _DIGEST.fullmatch(args.image_digest):
        raise SystemExit("image digest must be an immutable sha256 digest")

    output = Path(args.out)
    output.mkdir(parents=True, exist_ok=True)
    schema = TypeAdapter(Envelope).json_schema(mode="validation")
    schema.update(
        {
            "$id": "https://contracts.juntai.example/synthetic/worker/swp.v1.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Juntai Synthetic Worker Protocol v1",
            "x-juntai-canonicalization": "RFC 8785 JCS; contentDigest omitted while hashing",
            "x-juntai-framing": {
                "lengthPrefix": "uint32-big-endian",
                "maximumFrameBytes": MAX_FRAME_BYTES,
                "productionTransport": "juntai.platform.swp-stream/v1",
                "localDevelopmentSocket": SOCKET_PATH,
            },
            "x-juntai-production-worker-client": {
                "distribution": "juntai-platform-swp-stream==1.0.0",
                "factory": "juntai_platform_swp_stream:create_worker_client",
                "profile": "juntai.platform.swp-stream/v1",
                "externalContract": {
                    "manifestSha256": PLATFORM_ADAPTER_CONTRACT_SHA256,
                    "sourceCommit": PLATFORM_ADAPTER_SOURCE_COMMIT,
                    "sourceTree": PLATFORM_ADAPTER_SOURCE_TREE,
                    "wheelSha256": PLATFORM_STREAM_WHEEL_SHA256,
                },
                "resultAcceptance": {
                    "returnMeaning": "durable Platform result-outbox commit accepted",
                    "ambiguousFailureReplay": (
                        "fresh authenticated session; identical sequence, framedBytes, terminal"
                    ),
                    "terminalOrdering": "ResultAccepted before close",
                },
                "environment": [
                    "JUNTAI_SWP_TRANSPORT_FACTORY",
                    "JUNTAI_SWP_EXECUTOR_ADDRESS",
                    "JUNTAI_SWP_EXECUTOR_CA_FILE",
                    "JUNTAI_SWP_WORKLOAD_TOKEN_FILE",
                    "JUNTAI_SWP_CLAIM_ID",
                    "JUNTAI_SWP_CLAIM_GENERATION",
                    "JUNTAI_SWP_POD_UID",
                    "JUNTAI_SWP_CONTRACT_MANIFEST_FILE",
                    "JUNTAI_SWP_CONTRACT_MANIFEST_SHA256",
                ],
            },
        }
    )
    schema_path = output / "swp.v1.schema.json"
    schema_path.write_bytes(_canonical(schema))
    digest = hashlib.sha256(schema_path.read_bytes()).hexdigest()
    (output / "swp.v1.sha256").write_text(
        f"{digest}  swp.v1.schema.json\n", encoding="utf-8", newline="\n"
    )
    transport = {
        "schemaVersion": "juntai.synthetic.transport-spi/v1",
        "ownership": {
            "service": "canonical bytes, KES outbox leases/publication state, authenticated "
            "result/DLQ ingestion, inbox idempotency, lifecycle commits, and opaque ordering-key "
            "generation",
            "platform": "adapter, endpoints, credentials, queue resources, executor, opaque-key "
            "ordering, visibility receipts, and durable delivery/DLQ accounting",
        },
        "configuration": {
            "adapterDistribution": f"{TRANSPORT_DISTRIBUTION}=={TRANSPORT_VERSION}",
            "adapterFactory": TRANSPORT_FACTORY,
            "factoryEnvironment": "JUNTAI_QUEUE_TRANSPORT_FACTORY",
            "credentialFileEnvironment": "JUNTAI_QUEUE_CREDENTIAL_FILE",
            "credentialFile": QUEUE_CREDENTIAL_FILE,
            "credentialSchema": QUEUE_CREDENTIAL_SCHEMA,
            "credentialServerName": QUEUE_SERVER_NAME,
            "contractManifestFileEnvironment": "JUNTAI_QUEUE_CONTRACT_MANIFEST_FILE",
            "contractManifestSha256Environment": "JUNTAI_QUEUE_CONTRACT_MANIFEST_SHA256",
            "externalContract": {
                "manifestSha256": PLATFORM_ADAPTER_CONTRACT_SHA256,
                "sourceCommit": PLATFORM_ADAPTER_SOURCE_COMMIT,
                "sourceTree": PLATFORM_ADAPTER_SOURCE_TREE,
                "wheelSha256": PLATFORM_QUEUE_WHEEL_SHA256,
            },
            "endpointForm": (
                "grpcs://[literal-executor-Service-ClusterIP]:7444/<logical-channel>"
                f"?serverName={QUEUE_SERVER_NAME}"
            ),
            "endpointEnvironments": {
                "synthetic.worker.dispatch.v1": "JUNTAI_QUEUE_DISPATCH_ENDPOINT",
                "synthetic.worker.control.v1": "JUNTAI_QUEUE_CONTROL_ENDPOINT",
                "synthetic.worker.result.v1": "JUNTAI_QUEUE_RESULT_ENDPOINT",
                "synthetic.worker.dead-letter.v1": "JUNTAI_QUEUE_DEAD_LETTER_ENDPOINT",
            },
            "adapterStatus": "approved-platform-binding",
            "remoteReadiness": (
                "bindingMetadata authenticates the executor and proves descriptor, Kafka, "
                "Platform ledger, IAM, and policy readiness before any Synthetic KES lease"
            ),
            "contractManifestRequired": {
                "queueProtoPath": "queue/juntai.platform.queue.v1.proto",
                "queueDescriptorPath": "queue/descriptor.pb",
                "queueRpcService": "juntai.platform.queue.v1.QueueTransport",
                "queueRpcPort": 7444,
                "queueRpcTransport": "tls1.3-h2-mtls",
                "queueRpcResource": "urn:juntai:platform:queue-transport",
                "queueRpcAudience": "juntai-platform-queue-transport",
                "deliveryAuthority": "platform_worker_delivery_v1",
            },
        },
        "requiredMetadata": {
            "schemaVersion": QUEUE_BINDING_PROFILE,
            "channels": list(REQUIRED_CHANNELS),
            "provider": KAFKA_PRODUCT,
            "providerVersion": KAFKA_VERSION,
            "providerImageDigest": KAFKA_IMAGE_DIGEST,
            "deliverySemantics": "at-least-once-platform-ledger-authoritative",
            "visibilitySeconds": 60,
            "renewEverySeconds": 20,
            "maximumDeliveries": 5,
            "idempotentPublish": True,
            "deadLetter": True,
        },
        "operations": {
            "bindingMetadata": "return exact capability metadata or fail closed",
            "publish": (
                "channel, messageId, contentDigest, canonicalBytes, orderingKey -> publicationId"
            ),
            "receive": "channel, bounded limit, visibilitySeconds -> authenticated deliveries",
            "renew": "opaque receipt and bounded visibilitySeconds",
            "acknowledge": "opaque receipt after durable KES commit",
            "release": "opaque receipt; Platform computes authoritative full jitter",
            "reject": "opaque receipt and bounded stable reasonCode",
        },
        "deliveryMetadata": {
            "required": [
                "channel",
                "messageId",
                "contentDigest",
                "canonicalBytes",
                "opaqueReceipt",
                "deliveryCount",
                "leaseExpiresAt",
                "authenticatedProducer",
            ],
            "deadLetterAdditional": [
                "originalChannel",
                "originalDeliveryCount",
                "originalContentDigest",
                "terminalReasonCode",
                "ledgerEvidenceId",
            ],
            "serviceContentExcludes": [
                "opaqueReceipt",
                "brokerTimestamp",
                "providerOffset",
                "providerPartition",
            ],
        },
        "orderingKey": {
            "nonControl": "messageId",
            "control": (
                "lowercase hex sha256 of juntai.synthetic.control-order/v1 NUL tenantId NUL "
                "jobId NUL attemptId"
            ),
            "utf8Bytes": {"minimum": 1, "maximum": 256},
            "forbidden": ["whitespace", "NUL", "control characters"],
            "platformInterpretation": "opaque",
        },
        "prohibited": [
            "Platform adapter implementation or durable ledger code copied into the service",
            "Platform executor or adapter access to Synthetic KES",
            "service implementation of Platform durable delivery accounting",
            "worker queue endpoint, credential, client, or receipt",
            "callback route, mutable file exchange, or logical-channel bypass",
        ],
        "requiredConformanceEvidence": [
            "exact adapter distribution, factory, remote binding metadata, endpoint, mTLS "
            "credential, queue RPC descriptor, and delivery-authority profile",
            "same-tuple stable publicationId and different-digest conflict",
            "exact canonical-byte and opaque orderingKey pass-through",
            "receipt generation fencing for renew, acknowledge, release, and reject",
            "authenticated result and DLQ producer identity derived from mTLS",
            "durable hard-five delivery and exactly-one DLQ proof owned by Platform",
            "crash, rebalance, redelivery, acknowledgement ambiguity, and restart recovery",
        ],
    }
    transport_path = output / "queue-capability.v1.json"
    transport_path.write_bytes(_canonical(transport))
    transport_digest = hashlib.sha256(transport_path.read_bytes()).hexdigest()
    (output / "queue-capability.v1.sha256").write_text(
        f"{transport_digest}  queue-capability.v1.json\n", encoding="utf-8", newline="\n"
    )
    if args.source_commit is not None:
        manifest = {
            "schemaVersion": "juntai.synthetic.worker-protocol-release/v1",
            "protocol": PROTOCOL_VERSION,
            "serviceVersion": __version__,
            "sourceCommit": args.source_commit,
            "workerImageDigest": args.image_digest,
            "schema": {"path": "swp.v1.schema.json", "sha256": digest},
            "transportSpi": {
                "path": "queue-capability.v1.json",
                "sha256": transport_digest,
                "adapterStatus": "approved-platform-binding",
            },
            "canonicalization": "RFC 8785",
            "contentDigest": "sha256 of canonical envelope with contentDigest omitted",
            "localDevelopmentSocket": {
                "path": SOCKET_PATH,
                "owner": "root",
                "group": "juntai-worker",
                "mode": "0660",
                "maximumFrameBytes": MAX_FRAME_BYTES,
            },
            "channels": {
                "dispatch": "synthetic.worker.dispatch.v1",
                "control": "synthetic.worker.control.v1",
                "result": "synthetic.worker.result.v1",
                "deadLetter": "synthetic.worker.dead-letter.v1",
            },
            "delivery": {
                "visibilitySeconds": 60,
                "renewEverySeconds": 20,
                "maximumDeliveries": 5,
                "retryBaseSeconds": 5,
                "retryCapSeconds": 300,
                "terminationAllowanceSeconds": 60,
                "maximumLeaseSeconds": 21600,
            },
            "capabilities": [
                "canonical-envelope-digest",
                "cancel-sequence",
                "exact-artifact-references",
                "terminal-evidence",
            ],
            "minimumExecutorBinding": "juntai.platform.synthetic-executor/v1",
            "artifactMediaTypes": [INPUT_MEDIA_TYPE, EVIDENCE_MEDIA_TYPE],
        }
        (output / "generation-manifest.json").write_bytes(_canonical(manifest))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
