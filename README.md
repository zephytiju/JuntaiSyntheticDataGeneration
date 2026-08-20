# Juntai Synthetic Data Generation

This repository owns one generic FuseAPI asynchronous job service and the service side of
`juntai.synthetic.worker/v1` (SWP/v1). It accepts
a bounded versioned structural contract, selects a compatible provider, applies privacy policy and
quota reservations, generates a deterministic candidate dataset in ephemeral storage, optionally
runs an exact validator Artifact through an injected no-network sandbox, and publishes one
immutable dataset Artifact through `juntai-artifact-client` directly to OCI.

It never interprets domain schemas, imports domain packages, writes KingbaseES application data,
receives KES credentials, deploys previews, promotes data, or owns service-local IaC. The included
KingbaseES-compatible migration stores bounded job metadata and append-only transitions only.

## Public contract

- `POST /v1/jobs` requires `Idempotency-Key`; tenant authority comes only from verified IAM identity.
- `GET /v1/jobs/{job_id}` returns bounded state and failure evidence.
- `POST /v1/jobs/{job_id}:cancel` is idempotent and best effort before publication commit.
- `GET /v1/jobs/{job_id}/result` returns an exact immutable Artifact reference only after success.

Contract, request, dataset manifest, provenance, validator protocol, errors, and OpenAPI majors are
versioned. Payload bytes and OCI/KES credentials are never returned by this API.

## Verification

```bash
python -m venv .venv
JUNTAI_PLATFORM_REPOSITORY=/path/to/exact/JuntaiPlatformInfrastructure \
  sh scripts/prepare-external-dependencies.sh
.venv/bin/pip install --find-links .platform-adapters --find-links .iam-artifacts -e '.[test]'
.venv/bin/ruff check .
.venv/bin/pytest
.venv/bin/python -m build
```

Production runs must set `JUNTAI_WORKER_IMAGE_DIGEST` to the published GHCR digest, use mTLS for
the internal Artifact Registry, and inject OCI credentials through the standard workload path.
Every routed operation requires a Casdoor bearer access token with exact audience
`juntai.synthetic-data.api`; tenant identity comes from the verified human/delegated workload,
never a request body or header supplied by the caller.
Production authorization is pinned to `juntai-iam==1.1.0` (source
`72b481ed825c00d0bd96feca67790e90dc5ace9b`) and
`juntai-iam-contracts==1.1.1` (source
`a37b6d6daaba75efd8c15c19b440a3081ba761c5`). Before constructing the verifier,
the service verifies the installed contract manifest digest
`64dafb25c54d40320347c8661960d23ba524a2d3c102d112c08c95679d12db85`.
It imports the published verifier, middleware, principal model, and policy evaluator; it does not
copy IAM schemas or recreate peer-principal/delegation evaluation.

The API coordinator is the only writer of Synthetic job metadata in KES. It publishes immutable
worker input Artifacts, commits dispatch/control envelopes to the KES outbox atomically with job
state, verifies exact result/evidence Artifact coordinates, and atomically commits idempotent
worker events. The `relay` entry point owns KES outbox leasing/publication state and authenticated
result/DLQ inbox commits behind a transport-neutral, fail-closed SPI. Platform owns the durable
dispatch, control, result, and DLQ queues, the approved adapter, and the generic
executor Deployment that creates one separately fenced worker Job Pod per durable claim. The worker
initiates the authenticated remote SWP stream; the historical Unix socket is local-test framing only.
The queue and stream factories are keyword-only and consume exact read-only contract manifests plus
their lowercase SHA-256 values. Both fail closed unless the external Platform manifest digest is
`7d50a9e7b6733c88082ecb9e9a433801de69a7b1f99286137c69470e6c03216b` (source commit
`3dc2dd844194db8a6891590f7d088b437c34fc5f`, tree
`5996502910b04eda3a1ab56fd8d1f94a38e3d3de`). Queue delivery release carries only an opaque receipt; Platform
alone computes the authoritative 5–300-second cryptographic jitter from its durable ledger.
The relay's four queue endpoints are the same literal executor Service ClusterIP on port 7444 with
channel-specific paths and the exact executor TLS server name. The adapter is an authenticated
QueueTransport proxy: it has no direct Kafka, Platform-KES, in-memory-ledger, or fallback path, and
remote binding readiness must succeed before the Synthetic service can acquire a KES outbox lease.

The `worker` mode consumes only canonical length-prefixed SWP/v1 JSON frames and exact Artifact
inputs. It deliberately has no job-metadata KES DSN/secret/mount/network, queue client or token,
Synthetic API credential, or Kubernetes API credential. The Platform deployment must allow the
worker only the injected executor ClusterIP:7443 stream, Artifact Registry/OCI, OTel, and explicitly
declared provider endpoints; the API and worker
remain different runtime compositions even though `serve`, `relay`, `worker`, and `migrate` ship in
the same immutable wheel/image.

The explicit service-owned job-metadata KES migration command and its secret-file configuration,
locking, compatibility, exit, and rollback contract are specified in [MIGRATIONS.md](MIGRATIONS.md).

## Documentation capability publication

The last immutable service-owned documentation release is 1.2.0. The forward 1.3.0 material
under `documentation/` adds the relay, worker stream, and exact IAM 1.1 contract. It is manifested
and published only from the reviewed Synthetic source commit after exact Platform adapter/IAM
artifact verification, bearer-IAM OpenAPI generation, and FuseAPI 2.0.0 MCP descriptor generation.
The descriptor deliberately
contains no Tools because the documented service is HTTP-only; the bundle publishes MCP Resources
without inventing a second runtime surface.

Resolve, validate, build, and verify with the immutable
`JuntaiDocumentationCapabilityBundle` v1.0.0 wheel whose SHA-256 is
`82995a96601f8249ca85bfd51cfb5fe34c3a2d8608ff7b0d42c5004a59843c33`:

```bash
juntai-capability resolve --manifest documentation/manifest.yaml --lock documentation/capability.lock
juntai-capability validate --lock documentation/capability.lock
juntai-capability build --lock documentation/capability.lock --out dist/capability
```

For a completed release, CI repeats the locked build twice, verifies byte identity, compiles the
signed catalog input, and proves exact static-catalog selection. The forward 1.3.0
workflow must publish the immutable bundle, human and MCP projections, provenance, publication
result, pin, catalog input, checksums, SBOM, in-toto provenance, and GitHub build attestations. The
completed 1.0.0 and 1.2.0 documentation tags/releases are never moved or replaced.
