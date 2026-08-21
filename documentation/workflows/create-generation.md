# Create application preview data

Send `POST /v1/generations` with a bounded request and an `Idempotency-Key`. The authenticated IAM
identity supplies the tenant; the request supplies only generation rules and logical destinations.

For each record type, declare a positive count, generated fields, the destination schema/table, a
one-to-one field-to-column mapping, and the generated fields that authoritatively identify each
inserted row. Relations declare generated-field dependencies and deterministic write order.

The service treats those logical destinations as authoritative and performs no live-catalog
preflight. It generates in process, safely quotes the caller's identifiers, binds every value, and
asks KingbaseES to commit every application row, the generation result, and the exact written keys in
one transaction. A database rejection rolls the transaction back. A new commit returns `201`;
replaying the same key and request returns the same committed result with `200`. Reusing a key for
changed content fails with `IDEMPOTENCY_KEY_REUSED`.
