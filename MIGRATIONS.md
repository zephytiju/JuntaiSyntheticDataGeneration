# Synthetic application-KES migrations

This repository and its released wheel/image exclusively own the `juntai_synthetic_data` schema in
the same application KingbaseES cluster used for test-fleet preview data. Application schemas and
tables remain domain-owned. Synthetic stores bounded request/result identity and an exact written-key
ledger; it does not create or alter application tables.

The migration command is an explicit one-shot operation. Service startup never applies migrations.
Deployment supplies an owner-only DSN secret file for the Vangu-provided application cluster and
executes the exact released image:

```bash
export JUNTAI_SYNTHETIC_DATA_KES_DSN_FILE=/run/secrets/synthetic-application-kes-dsn
export JUNTAI_SOURCE_REVISION=<40-character-released-source-commit>
export JUNTAI_SERVICE_IMAGE_DIGEST=sha256:<64-hex-released-image-digest>
juntai-synthetic-data migrate
```

The API runtime uses the same DSN secret and additionally requires the exact admission binding:

```bash
export JUNTAI_SYNTHETIC_DATA_TEST_FLEET=true
juntai-synthetic-data serve
```

Missing, differently cased, padded, or otherwise different marker values are rejected before the
DSN secret is read or the service is constructed. `JUNTAI_ENVIRONMENT` is not an admission input.

The command verifies the ordered manifest and SQL checksums before connecting, requires KingbaseES
V009R001C010, takes a transaction-scoped advisory lock, and records migration IDs, checksums, source
revision, service version, and image digest in `juntai_synthetic_data.schema_migrations`.

Migrations `0001_jobs` and `0002_worker_protocol` remain immutable historical inputs required to
upgrade released 1.0.0–1.2.0 installations. Forward migration `0003_synchronous_generations`
atomically removes the withdrawn async job/worker tables and creates `generations` and
`generation_rows` with tenant RLS. Empty application, historical baseline adoption, forward upgrade,
repeat execution, and concurrent invocations are all transactional and idempotent. A failure rolls
back the entire invocation.

Use `juntai-synthetic-data migrate --check` for a non-mutating compatibility check. Exit codes are
stable: `0` current/applied, `2` invalid configuration, `3` incompatible state/checksum/downgrade
safety failure, `4` database or transactional execution failure, and `5` pending migrations under
`--check`.

Migrations are forward-only. Rollback means re-pinning a previously published compatible service
image. Reversing schema changes requires an operator-approved restore or a new reviewed forward
migration; the command never performs an implicit destructive downgrade.

The licensed acceptance harness covers empty/repeat/concurrent migration, forced whole-invocation
rollback, upgrade from the released historical schema, application-schema preconditions, atomic
cross-schema inserts plus metadata/key ledger, identical replay and lost-response recovery, exact-key
delete and conflict rollback, tenant RLS, database restart, and a service identity with no Platform
database authority.
