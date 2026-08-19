# Stable failures and recovery

- `CONTRACT_INVALID`: correct the structural contract; do not retry unchanged input.
- `PROVIDER_UNSUPPORTED` or `DETERMINISTIC_SEED_INCOMPATIBLE`: choose supported declared requirements.
- `POLICY_DENIED`: remove disallowed input or obtain the required authorization; do not bypass policy.
- `QUOTA_EXCEEDED`: reduce bounds or wait for the quota window. The failure is retryable only as reported.
- `IDEMPOTENCY_KEY_REUSED`: use the original identical request or a new key for changed content.
- `VALIDATOR_FAILED` or `SANDBOX_VIOLATION`: correct the exact validator or candidate contract. No dataset is published.
- `OUTPUT_LIMIT_EXCEEDED`: reduce record or byte bounds. Temporary data is removed.
- `PUBLICATION_FAILED`, `DEPENDENCY_UNAVAILABLE`, or `DEPENDENCY_DEADLINE`: retry identical content with bounded backoff only when `retryable` is true.
- `JOB_NOT_FOUND`: verify tenant authority and exact job ID.
- `JOB_NOT_SUCCEEDED`: continue polling or inspect the terminal failure before requesting a result.
- `JOB_CANCELLED`: create a new job if generation is still required.
- `CONCURRENCY_CONFLICT`: reread status and retry the intended operation against current state.
- `PROTOCOL_UNSUPPORTED` or `ENVELOPE_INVALID`: quarantine the delivery; deploy an executor that
  supports SWP/v1 rather than rewriting the frame.
- `ENVELOPE_DIGEST_MISMATCH`, `IDENTITY_MISMATCH`, `TENANT_MISMATCH`, or `RESULT_CONFLICT`: fail
  closed and investigate transport/integrity evidence; never acknowledge as success.
- `ATTEMPT_STALE` or `RESULT_DUPLICATE`: do not reapply job state. Record cleanup evidence for any
  newly published orphan Artifact.
- `WORKER_EXITED` or retryable `DEPENDENCY_UNAVAILABLE`: redeliver with full-jitter backoff inside
  the original deadline and delivery budget.
- `DELIVERY_EXHAUSTED`: inspect the dead-letter record and immutable evidence. Automated retries
  stop after five deliveries.
- `ARTIFACT_INTEGRITY_FAILED`: do not associate the output; exact coordinates, tenant, media type,
  producer build, or digest verification failed.

Never recover by weakening digest checks, switching to a mutable input, querying private storage, or treating a partial upload as a successful result.
