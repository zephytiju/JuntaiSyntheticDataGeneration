# Synthetic job-metadata KES migrations

This repository and its released wheel/image exclusively own the
`juntai_synthetic_data` KingbaseES schema. The schema contains bounded asynchronous job metadata,
state transitions, SWP attempts, canonical dispatch/control outbox records, idempotent result
inbox records, and stale-output cleanup evidence. It does not contain generated datasets,
transport-provider delivery ledgers, queue resources,
product-domain tables, target KES
data, deployment IaC, or Documentation Capability data.

The migration is an explicit one-shot operation. API, relay, and worker startup never applies migrations.
Platform Infrastructure supplies the KES connection as an owner-only secret file and orchestrates
the exact released command; it does not copy or interpret the SQL.

```bash
export JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE=/run/secrets/synthetic-job-metadata-kes-dsn
export JUNTAI_SOURCE_REVISION=<40-character-released-source-commit>
export JUNTAI_SERVICE_IMAGE_DIGEST=sha256:<64-hex-released-image-digest>
juntai-synthetic-data migrate
```

The same command is present beside `serve`, `relay`, and `worker` in the service wheel/image. It reads the
ordered `migrations/manifest.v1.json`, verifies every SQL checksum before connecting, requires
KingbaseES V009R001C010, takes a transaction-scoped advisory lock, and records applied IDs,
checksums, source revision, service version, and image digest in
`juntai_synthetic_data.schema_migrations`.

Use `juntai-synthetic-data migrate --check` for a non-mutating compatibility check. A released
1.0.0 schema with the exact expected tables, columns, RLS state, and policies is adopted into the
ledger without re-running SQL. Release 1.2.0 applies `0002_worker_protocol` after the released
1.1.0 `0001_jobs` ledger baseline. Release 1.3.0 applies `0003_transport_relay` after the immutable
1.2.0 ledger. The additive migration adds only service-owned outbox publication leases/retry state
and dead-letter inbox evidence; it does not add Platform claim/delivery accounting. Empty-database
application, baseline adoption, and all future
migrations run transactionally. A failed migration rolls back the entire invocation. Concurrent
runs serialize on the advisory lock; repeat execution is idempotent.

Exit codes are stable: `0` means current or applied, `2` invalid configuration, `3` incompatible
state/checksum/downgrade safety failure, `4` database or transactional execution failure, and `5`
pending migrations under `--check`.

Migrations are forward-only. Rollback first stops admission and the relay, drains or reconciles
in-flight SWP attempts, and re-pins only a previously published image whose declared compatibility
includes the current `0003_transport_relay` ledger. An emergency 1.2.0 image remains unavailable
for new execution because its migration verifier correctly rejects the unknown forward ledger; it
must not delete relay columns/table or restore the historical KES-polling worker. The command never
performs an implicit or destructive downgrade. Restore from an operator-approved backup or publish
a new reviewed forward migration if a schema change must be reversed.

`scripts/run-real-kes-acceptance.sh` runs the complete acceptance matrix against the approved,
digest-pinned V009R001C010 image. GitHub-hosted runners cannot acquire that licensed image; the
immutable service release therefore accepts only a complete matrix result embedded in its annotated
tag, verifies it against the already-published exact source image digest, and publishes the result
beside the migration manifest, checksums, SBOM, provenance, and GitHub attestations.

The matrix covers empty/repeat/concurrent execution, whole-invocation rollback on a forced failure,
upgrade through the exact 1.1.0 and 1.2.0 ledgers, tenant RLS across every SWP table, atomic API
outbox/result/dead-letter state, relay lease expiry and restart recovery, API/worker startup after
migration, database restart, and a separate internal worker network
that cannot resolve or connect to the KES container. The harness uses bounded task-specific
disposable container, network, volume, and owner-only secret-file names, clears any stale prior run
before startup, and removes them through an exit trap.
