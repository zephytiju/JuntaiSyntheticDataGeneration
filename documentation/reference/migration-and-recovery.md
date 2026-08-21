# Migration and recovery

The service-owned migration command upgrades `juntai_synthetic_data` to the synchronous generation
metadata and exact written-key ledger. It retains immutable historical migrations for upgrade
verification and removes their obsolete job/worker tables only in the reviewed forward migration.
Application schemas and rows are never created or migrated by this repository.

Migration and runtime use the same Vangu-provided application KingbaseES cluster through a
secret-file DSN. Migrations are ordered, checksum-verified, transaction-locked, and forward-only.
Re-pin a compatible prior service image for application rollback; schema reversal requires a new
reviewed forward migration or an operator-approved restore.

The runtime retries only transient KingbaseES transaction/connectivity errors within the request
deadline. Idempotency and the database transaction provide recovery after a lost client response or
service restart; there is no background queue to drain.
