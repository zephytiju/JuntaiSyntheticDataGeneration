# Juntai Synthetic Data Generation

This repository owns one generic FuseAPI asynchronous job service and its worker mode. It accepts
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
