# Juntai Synthetic Data Generation

This repository owns a small synchronous service for generating preview data in Juntai test fleets.
The service validates bounded structural generation rules, generates deterministic records in
process, and atomically inserts them into caller-declared schemas/tables in the fleet's one application
KingbaseES cluster. It records generation metadata and exact written keys in its own schema in the
same transaction.

The service has no asynchronous jobs, worker runtime, queue, Kafka, SWP, dispatcher, executor,
delivery ledger, DLQ, or Artifact/object-storage result path. Platform infrastructure may deploy the
immutable service and inject the application-database binding, but it is not a service runtime or
data dependency.

## Unpublished V1 API

- `POST /v1/generations` synchronously generates and commits application records. It requires an
  `Idempotency-Key`; identical replay returns the original committed result.
- `GET /v1/generations/{generation_id}` recovers committed/deleted metadata after a lost response.
- `DELETE /v1/generations/{generation_id}` atomically deletes only the exact keyed rows written by
  that generation. Repeated deletion is idempotent.

Each record type declares its logical `{schema, table, columns, key_fields}` destination. Requests
cannot carry database addresses, credentials, tenant identity, arbitrary connection options, or raw
SQL. Authenticated internal callers are authoritative for those logical destinations. Synthetic
quotes identifiers through the database driver and binds every value; KingbaseES enforces object
existence, columns, types, defaults, keys, relations, grants, constraints, and RLS when the atomic
transaction executes.

## Safety boundary

The service runs only in test fleets against the single Vangu-provided application database shared
by Axiom and Lattice schemas. It does not write Platform data or production application databases.
Tenant identity comes only from verified IAM context; Synthetic metadata is protected by RLS. The
published IAM tuple is `juntai-iam==1.1.0` and `juntai-iam-contracts==1.1.1`; the service verifies the
contracts manifest and imports, rather than recreates, IAM semantics.

Authenticated internal callers are the sole source of logical destination names. Deployment must
set `JUNTAI_SYNTHETIC_DATA_TEST_FLEET=true`. The `serve` entry point compares this binding literally
and rejects a missing value or any value other than exact lowercase `true` before constructing the
application-database connector or service. Generic `JUNTAI_ENVIRONMENT` metadata does not satisfy
this admission check.

## Verification

```bash
python -m venv .venv
.venv/bin/pip install -e '.[test]'
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/pytest -q
.venv/bin/python -m build
```

`scripts/run-real-kes-acceptance.sh` exercises migration, cross-schema atomic writes, replay,
lost-response recovery, exact-key deletion, rollback, restart recovery, and tenant isolation against
the approved licensed KingbaseES image. See [MIGRATIONS.md](MIGRATIONS.md).

## Documentation capability

`documentation/` contains the aligned human, agent-resource, HTTP/OpenAPI, and provenance inputs for
the forward 1.3.0 documentation release. The MCP descriptor intentionally has no Tools because this
service exposes only HTTP APIs. Final documentation publication binds the exact merged service source
commit, checksums, SBOM, in-toto provenance, and immutable GitHub release; completed earlier tags and
releases are never moved or replaced.
