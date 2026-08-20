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
.venv/bin/pip install -e '.[test]'
.venv/bin/ruff check .
.venv/bin/pytest
.venv/bin/python -m build
```

Production runs must set `JUNTAI_WORKER_IMAGE_DIGEST` to the published GHCR digest, use mTLS for
the internal Artifact Registry, and inject OCI credentials through the standard workload path.
Every routed operation requires a Casdoor bearer access token with exact audience
`juntai.synthetic-data.api`; tenant identity comes from the verified human/delegated workload,
never a request body or header supplied by the caller.

The API coordinator is the only writer of Synthetic job metadata in KES. It publishes immutable
worker input Artifacts, commits dispatch/control envelopes to the KES outbox atomically with job
state, verifies exact result/evidence Artifact coordinates, and atomically commits idempotent
worker events. Platform owns the durable dispatch, control, result, and DLQ queues and the generic
executor sidecar that relays canonical frames to the service worker over
`/var/run/juntai-worker/swp-v1.sock`.

The `worker` mode consumes only canonical length-prefixed SWP/v1 JSON frames and exact Artifact
inputs. It deliberately has no job-metadata KES DSN/secret/mount/network, queue client or token,
Synthetic API credential, or Kubernetes API credential. The Platform deployment must allow the
worker only its Unix socket plus the released Artifact Registry/OCI endpoints; the API and worker
remain different runtime compositions even though `serve`, `worker`, and `migrate` ship in the
same immutable wheel/image.

The explicit service-owned job-metadata KES migration command and its secret-file configuration,
locking, compatibility, exit, and rollback contract are specified in [MIGRATIONS.md](MIGRATIONS.md).

## Documentation capability publication

The service-owned reviewed documentation graph lives under `documentation/`. It is bound to an
exact reviewed 1.2.0 source commit, the bearer-IAM OpenAPI digest, and the exact
FuseAPI 2.0.0 MCP descriptor. The descriptor deliberately contains no Tools because the documented
release is HTTP-only; the bundle publishes MCP Resources without inventing a second runtime surface.

Resolve, validate, build, and verify with the immutable
`JuntaiDocumentationCapabilityBundle` v1.0.0 wheel whose SHA-256 is
`82995a96601f8249ca85bfd51cfb5fe34c3a2d8608ff7b0d42c5004a59843c33`:

```bash
juntai-capability resolve --manifest documentation/manifest.yaml --lock documentation/capability.lock
juntai-capability validate --lock documentation/capability.lock
juntai-capability build --lock documentation/capability.lock --out dist/capability
```

CI repeats the locked build twice, verifies byte identity, compiles the signed catalog input, and
proves exact static-catalog selection. The forward `synthetic-data-docs-v1.2.0` workflow publishes
the immutable bundle, human and MCP projections, provenance, publication result, pin, catalog input,
checksums, SBOM, in-toto provenance, and GitHub build attestations. The completed
`synthetic-data-docs-v1.0.0` release is never moved or replaced.
