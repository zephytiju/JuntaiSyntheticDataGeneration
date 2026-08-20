# Service-owned SWP transport relay

The `relay` entry point is the API-side bridge between Synthetic's KES truth and the four generic
Platform queue capabilities. It is not the Platform executor, a queue resource definition, a
delivery ledger, or a worker transport. Synthetic owns the relay loop and the `QueueTransport` SPI;
Platform supplies `juntai-platform-queue-kafka==1.0.0`. The Python factory/session callable addendum
and exact local conformance artifacts are externally supplied and are not reimplemented here. The
service pins manifest SHA-256
`7d50a9e7b6733c88082ecb9e9a433801de69a7b1f99286137c69470e6c03216b`, Platform source commit
`3dc2dd844194db8a6891590f7d088b437c34fc5f`, source tree
`5996502910b04eda3a1ab56fd8d1f94a38e3d3de`, queue wheel SHA-256
`d787126955c11e27ec05ca7c22e8f945cf0a89bf989c1e438ee86640e56622dc`, and stream wheel SHA-256
`cba7a87783cd804f5e496473f0961757c27a0455b946ed82041a7d1d01ef6033`.

## Synthetic-owned durable behavior

- Lease a bounded batch of unpublished `worker_outbox` rows with `FOR UPDATE SKIP LOCKED`, an
  opaque lease token, a 60-second expiry, and a monotonically increasing publish-attempt count.
- Decode and revalidate the stored canonical bytes before publication. Channel, tenant, job,
  attempt, message, sequence, and content digest must match the KES row.
- Publish the exact stored bytes with `messageId`, `contentDigest`, and `orderingKey`. Dispatch uses
  `messageId`; control uses the 64 lowercase hexadecimal SHA-256 of the exact domain-separated bytes
  `juntai.synthetic.control-order/v1\0<tenant>\0<job>\0<attempt>`. The key reveals no component and
  Platform may order but never decode or derive it. Mark `published_at` and
  `platform_publication_id` only after the transport confirms
  the same identity and digest.
- Treat a broker acknowledgement followed by a failed KES mark as a safe replay. The next relay
  publishes the same bytes and idempotency identity after the KES lease expires.
- Lease result and dead-letter deliveries for 60 seconds and renew every 20 seconds while the
  service verifies identity, content, Artifact references, and KES state.
- Acknowledge a result only after the result inbox disposition and every valid lifecycle change
  commit in one KES transaction. Request release for transient failures and reject only under the
  transport contract. Platform's durable ledger—not service state—owns retry timing and generations
  one through five and the unique fifth-failure DLQ transition.
- Commit dead-letter identity, digest, original bytes, producer identity, delivery evidence, and
  lifecycle disposition transactionally. Exact replays are idempotent; changed content conflicts.

## Exact transport-neutral SPI

The generated `contracts/worker-protocol/queue-capability.v1.json` is the machine-readable contract.
The Python SPI has these operations:

| Operation | Service input | Required result |
| --- | --- | --- |
| `binding_metadata` | none | Exact profile, four channels, provider compatibility, visibility, retry/DLQ, and idempotent-publish facts |
| `publish` | channel, message ID, content digest, canonical bytes, opaque ordering key | Same message ID/digest plus stable bounded publication ID |
| `receive` | result or DLQ channel, bounded batch, 60-second visibility | Canonical bytes, opaque receipt, delivery count, expiry, and authenticated producer |
| `renew` | opaque receipt and bounded visibility | Generation-fenced renewal; no service-content mutation |
| `acknowledge` | opaque receipt | Generation-fenced settlement after KES commit |
| `release` | opaque receipt | Generation-fenced retry request; Platform alone computes durable cryptographic full-jitter timing |
| `reject` | opaque receipt and bounded stable reason | Generation-fenced permanent-failure request |

An approved adapter must pass this seven-operation conformance checklist without changing the SPI:

1. `binding_metadata`: exact byte-for-byte metadata fixture passes; every missing or changed fact
   prevents relay startup before a KES lease or queue operation.
2. `publish`: two calls with the same message ID, digest, bytes, and ordering key return the same
   publication ID; the same ID with changed digest fails closed; control without the exact stable
   Synthetic key fails; a broker success/KES-mark failure is safely replayed.
3. `receive`: only result/DLQ are available to the Synthetic principal; batches are bounded; every
   delivery provides original canonical bytes, count, expiry, opaque receipt, and authenticated
   producer; spoofed or missing identity fails closed.
4. `renew`: a long KES/Artifact verification renews before 20 seconds, never beyond the original
   deadline plus termination allowance or six hours, and a failed renewal never becomes an ack.
5. `acknowledge`: result ack is observable only after the inbox/state transaction; a crash before
   commit redelivers, and a crash after commit resolves through the stored duplicate disposition.
6. `release`: transient queue/KES failure preserves bytes and attempt identity. The service passes
   only the opaque receipt; Platform atomically applies cryptographic full jitter in 5–300 seconds
   and increments its durable claim generation on the next committed claim.
7. `reject`: permanent protocol/authentication failures stay inside Platform's same five-generation
   state machine. DLQ delivery preserves the original bytes/digest and includes authenticated
   original channel, count=5, terminal reason, and ledger-evidence identity.

Runtime configuration maps to the SPI without interpreting transport topology:

| Required adapter field | Current code/config mapping |
| --- | --- |
| Approved adapter identity | `JUNTAI_QUEUE_TRANSPORT_FACTORY=juntai_platform_queue_kafka:create_transport`; exact distribution `juntai-platform-queue-kafka==1.0.0` |
| Dispatch endpoint/address | `JUNTAI_QUEUE_DISPATCH_ENDPOINT`; passed opaquely to the adapter |
| Control endpoint/address | `JUNTAI_QUEUE_CONTROL_ENDPOINT`; passed opaquely to the adapter |
| Result endpoint/address | `JUNTAI_QUEUE_RESULT_ENDPOINT`; passed opaquely to the adapter |
| Dead-letter endpoint/address | `JUNTAI_QUEUE_DEAD_LETTER_ENDPOINT`; passed opaquely to the adapter |
| Projected workload credential | Exact mode-0400 `/var/run/secrets/juntai/queue-binding/credentials.json`, schema `juntai.platform.queue-credential/v1`; never copied into an envelope or KES |
| Immutable callable contract | `JUNTAI_QUEUE_CONTRACT_MANIFEST_FILE` plus exact lowercase `JUNTAI_QUEUE_CONTRACT_MANIFEST_SHA256`; verified before service/KES construction |
| Provider compatibility | `binding_metadata` proves Apache Kafka `4.1.1`, image digest `sha256:0bc1bb…d40`, and the exact queue-binding profile |
| Publish idempotency | `publish(messageId, contentDigest, canonicalBytes, orderingKey)`; changed digest for one ID must fail closed |
| Result authentication | Every result delivery carries the queue-authenticated producer workload used by envelope verification |
| Receipt lifecycle | `receive`, `renew`, `acknowledge`, `release`, and `reject`; receipts stay transport metadata |
| DLQ preservation | DLQ deliveries add original channel, original count=5, original digest, terminal reason, and ledger-evidence ID while preserving bytes |

## Fail-closed readiness

The relay will not start when the exact factory/version, any exact
`grpcs://[literal-executor-Service-ClusterIP]:7444/<logical-channel>?serverName=swp-executor.juntai-platform.svc.cluster.local`
endpoint, the shared endpoint authority, mode-0400 credential binding/schema, external QueueTransport
descriptor/profile, or capability metadata is missing. Direct Kafka, KES, DNS, localhost, public,
alternate-port/server-name, credential-bearing, and extra-query endpoints are rejected. The projected
certificate identifies the `synthetic-relay` service principal and is scoped to resource
`urn:juntai:platform:queue-transport` and audience `juntai-platform-queue-transport`; it is not a Kafka
or Platform-KES credential.

`binding_metadata` is the authenticated remote readiness check and must succeed before the Synthetic
service or repository is constructed and before any KES outbox lease. It proves the four channels,
at-least-once Platform-ledger-authoritative delivery, idempotent publish, 60-second visibility,
20-second renewal, maximum five deliveries, DLQ preservation, and Apache Kafka 4.1.1 compatibility.
The proxy has no direct-broker, KES, in-memory-ledger, or delivery-counter fallback. The supplied
adapter/descriptor artifacts pass exact local backend conformance. Production startup additionally
requires the separately published IAM 1.1 tuple and a valid authenticated credential; artifact
presence alone is not deployed admission evidence.

The adapter may not expose Synthetic KES to Platform, add Platform durable delivery accounting to this
service, rewrite canonical bytes, invent a callback route, bypass a logical channel, or enter the
worker import/environment/mount/network closure.

## Production worker stream boundary

Production does not expose `/var/run/juntai-worker/swp-v1.sock`; that path is retained only for
isolated unit/local-development framing tests. Production uses
`juntai-platform-swp-stream==1.0.0` and `juntai_platform_swp_stream:create_worker_client` over the
worker-initiated TLS 1.3 HTTP/2 Attach stream to the injected executor ClusterIP:7443, pinned CA,
exact server name, and Pod-bound audience token. The worker receives no queue endpoint, receipt,
credential, KES/API/Kubernetes credential, arbitrary callback, DNS/world route, or Platform ledger
type. Production parses exactly `JUNTAI_SWP_TRANSPORT_FACTORY`, `JUNTAI_SWP_EXECUTOR_ADDRESS`,
`JUNTAI_SWP_EXECUTOR_CA_FILE`, `JUNTAI_SWP_WORKLOAD_TOKEN_FILE`, `JUNTAI_SWP_CLAIM_ID`,
`JUNTAI_SWP_CLAIM_GENERATION`, `JUNTAI_SWP_POD_UID`, `JUNTAI_SWP_CONTRACT_MANIFEST_FILE`, and
`JUNTAI_SWP_CONTRACT_MANIFEST_SHA256`. The factory and manifest are verified before Artifact or
engine construction. `SocketWorker.process` remains the local framing/engine primitive; a thin
service-owned `recv`/`sendall` bridge carries complete frames over the authenticated Platform
session without implementing TLS, Auth, queue, ledger, retry, or receipt behavior. `send_result`
returns only after durable `ResultAccepted`. Timeout or link loss permits one fresh authenticated
session for the same claim and one byte-identical pending-tuple resend after the reissued dispatch is
also byte-identical. Terminal acceptance is observed before close; no Platform acknowledgement or
ledger state is parsed by Synthetic.
