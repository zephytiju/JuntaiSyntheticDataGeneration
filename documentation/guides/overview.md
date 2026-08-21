# Synthetic Data Generation service

Synthetic Data Generation is a small synchronous service used only in a Juntai test fleet. It
creates deterministic preview records and inserts them into the single application KingbaseES
cluster supplied with that fleet. Application domains share that cluster while owning distinct
schemas and tables.

The service runs generation, validation, application inserts, and its own metadata bookkeeping in
one process. It does not use a worker, queue, dispatcher, executor, Kafka, Artifact storage, or a
Platform runtime. Platform infrastructure may deploy the immutable service image and supply its
application-database binding, but it does not implement Synthetic behavior.

Every request names logical schema, table, generated-field-to-column mappings, and key fields.
Database addresses, credentials, raw SQL, and tenant authority are never accepted in the API.
All requested application rows and the service's generation metadata/key ledger commit in one
database transaction or do not commit at all.

Use `POST /v1/generations` to generate and commit records, `GET
/v1/generations/{generation_id}` to recover a lost response, and `DELETE
/v1/generations/{generation_id}` to delete exactly the rows written by that generation.
