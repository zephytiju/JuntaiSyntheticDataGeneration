# Recover or delete a generation

`GET /v1/generations/{generation_id}` returns the committed result, including request, contract, and
data digests; provider identity; destinations and row counts; seed; and timestamps. Use it after a
client timeout or lost response instead of submitting changed content under the same idempotency key.

`DELETE /v1/generations/{generation_id}` deletes only the rows named by the service-owned written-key
ledger. It deletes child rows before parents, marks the generation `DELETED`, and commits those
changes atomically. Repeating DELETE returns the same deleted result. If an application row is
missing, changed, or still referenced outside the generation, deletion fails and rolls back without
partially removing the generation.

DELETE is not asynchronous cancellation. Once POST returns, the generation is already committed.
