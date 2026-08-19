# Create a bounded generation job

1. Obtain a human or delegated bearer token with audience `juntai.synthetic-data.api`, scope
   `synthetic-data:jobs`, and authorization for action `create` on `synthetic-data/jobs`.
2. Construct `juntai.synthetic-data.request/v1` with one generic `GenerationContract`, a non-empty seed, provider requirements, and policy.
3. Set `Idempotency-Key` to a stable opaque value of at most 200 characters. Reuse it only for the identical canonical request.
4. Call `POST /v1/jobs/` with JSON content.
5. Persist the returned `job_id`, `request_digest`, state, and quota reservation for polling and
   audit. Acceptance means the API has atomically committed the queued attempt and SWP dispatch
   outbox after publishing the exact worker-input Artifact.

The service accepts the same key and identical canonical request as a replay of the same job. Reusing the key with different content fails with `IDEMPOTENCY_KEY_REUSED`.

Schema-only generation is the default: use `source_examples: none`. A minimized source-derived request requires an explicit `authorization_reference` and remains subject to classification policy.

Do not include credentials, target namespaces, executable expressions, unbounded counts, or mutable Artifact aliases in the request.
