# Synthetic Worker Protocol v1

`juntai.synthetic.worker/v1` is the only supported execution boundary. Platform owns durable queues
named `synthetic.worker.dispatch.v1`, `synthetic.worker.control.v1`,
`synthetic.worker.result.v1`, and `synthetic.worker.dead-letter.v1`. Synthetic owns the canonical
dispatch, cancel, worker-event, Artifact, identity, resource, retry, and stable-error schemas.

The generic Platform executor commits a fenced claim and starts one digest-pinned worker Job Pod.
The worker initiates the approved TLS 1.3 HTTP/2 Attach stream to the injected executor
ClusterIP:7443 with exact server name, pinned CA, and Pod-bound audience token. Each Data message
contains exactly one existing frame: a four-byte unsigned big-endian length followed by at most 1,048,576 bytes of canonical
RFC 8785 UTF-8 JSON. Unknown members, duplicate members, non-canonical bytes, unsupported majors,
digest mismatch, sequence gap/replay, stale claim generation, or transport/producer identity
mismatch fail closed. `/var/run/juntai-worker/swp-v1.sock` is local-test framing only.

`WorkerSession.send_result(sequence, framed_bytes, terminal, timeout)` succeeds only after the
executor has durably committed the exact complete-frame SHA-256 tuple and returned
`ResultAccepted` (executor message tag 5); local enqueue is never acceptance. If that acknowledgement
times out or the link becomes unavailable, the worker closes the old client, performs fresh Auth and
Accepted for the same live claim generation, requires the dispatch frame to be byte-identical, and
resends exactly the pending `(sequence, framed_bytes, terminal)` once. It never reruns the engine,
changes the tuple, parses the acknowledgement or Platform ledger, or continues after a second
failure. A terminal acknowledgement precedes session/client close.

Dispatch carries exact tenant, job, attempt, sequence, deadline, correlation, input Artifact,
request digest, provider/version, worker image, capabilities, executor binding, resource envelope,
and idempotency digest. Worker events carry lease, stage/progress, observed cancellation sequence,
image/capabilities, evidence counters, and—when terminal—outcome, times, consumed input digest,
immutable execution evidence, stable error, and successful dataset Artifact/counts.

Queue visibility is 60 seconds and the executor renews every 20 seconds. Platform's durable generic
ledger commits claim generations one through five before execution, applies full-jitter retry timing,
creates exactly one DLQ outbox on the fifth failure, and forbids a sixth claim. Synthetic receives
authenticated result/DLQ evidence and acknowledges only after its KES inbox/lifecycle transaction.
Cancellation is monotonic;
the executor allows 30 seconds for graceful worker exit and a 60-second termination allowance.

The API outbox makes dispatch/control publication replayable. The result inbox makes event IDs
idempotent and detects digest conflicts. On restart, the API reconciles pending outbox records and
nonterminal attempts; the executor relies on its durable ledger, queue redelivery, and fenced claim
identity. A new
worker image is rolled out only after protocol/real-KES/isolation acceptance. Rollback re-pins a
compatible prior image; migrations remain forward-only and no component performs a destructive
implicit downgrade.
