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

Never recover by weakening digest checks, switching to a mutable input, querying private storage, or treating a partial upload as a successful result.
