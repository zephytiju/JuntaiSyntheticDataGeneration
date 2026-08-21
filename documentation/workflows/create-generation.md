# Create application preview data

Send `POST /v1/generations` with a bounded request and an `Idempotency-Key`. The authenticated IAM
identity supplies the tenant; the request supplies only generation rules and logical destinations.

For each record type, declare a positive count, generated fields, the destination schema/table, a
one-to-one field-to-column mapping, and the generated fields that map to an existing unique database
key. Relations declare which generated fields must match existing foreign keys between destinations.

The service validates the deployment allowlist and live database catalog before generation. It then
generates in process and commits every application row, the generation result, and the exact written
keys in one transaction. A new commit returns `201`; replaying the same key and request returns the
same committed result with `200`. Reusing a key for changed content fails with
`IDEMPOTENCY_KEY_REUSED`.
