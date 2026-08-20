# Synthetic Worker Protocol v1

`juntai.synthetic.worker/v1` is the only supported execution boundary. Platform owns durable queues
named `synthetic.worker.dispatch.v1`, `synthetic.worker.control.v1`,
`synthetic.worker.result.v1`, and `synthetic.worker.dead-letter.v1`. Synthetic owns the canonical
dispatch, cancel, worker-event, Artifact, identity, resource, retry, and stable-error schemas.

The generic executor sidecar reads dispatch/control deliveries and starts the digest-pinned worker.
It uses `/var/run/juntai-worker/swp-v1.sock`, owned by `root:juntai-worker` with mode `0660`.
Each frame is a four-byte unsigned big-endian length followed by at most 1,048,576 bytes of canonical
RFC 8785 UTF-8 JSON. Unknown members, duplicate members, non-canonical bytes, unsupported majors,
digest mismatch, or transport/producer identity mismatch fail closed.

Dispatch carries exact tenant, job, attempt, sequence, deadline, correlation, input Artifact,
request digest, provider/version, worker image, capabilities, executor binding, resource envelope,
and idempotency digest. Worker events carry lease, stage/progress, observed cancellation sequence,
image/capabilities, evidence counters, and—when terminal—outcome, times, consumed input digest,
immutable execution evidence, stable error, and successful dataset Artifact/counts.

Queue visibility is 60 seconds and the executor renews every 20 seconds. It acknowledges dispatch
only after the API durably commits the terminal result disposition. Retryable failures use full
jitter from zero through `min(300, 5 * 2^(delivery_count-1))` seconds, at most five deliveries,
within the original deadline and six-hour maximum lease. Exhausted/non-retryable deliveries go to
the DLQ with the original canonical bytes and bounded failure evidence. Cancellation is monotonic;
the executor allows 30 seconds for graceful worker exit and a 60-second termination allowance.

The API outbox makes dispatch/control publication replayable. The result inbox makes event IDs
idempotent and detects digest conflicts. On restart, the API reconciles pending outbox records and
nonterminal attempts; the executor relies on queue redelivery and exact attempt identity. A new
worker image is rolled out only after protocol/real-KES/isolation acceptance. Rollback re-pins a
compatible prior image; migrations remain forward-only and no component performs a destructive
implicit downgrade.
