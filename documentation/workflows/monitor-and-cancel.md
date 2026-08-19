# Monitor or cancel a job

Poll `GET /v1/jobs/{job_id}` with bounded backoff. The response returns state, stage, timestamps, request digest, optimistic-concurrency version, bounded quota details, and a stable failure when present.

The normal sequence is `ACCEPTED`, `POLICY_CHECK`, `QUEUED`, `RUNNING`, `VALIDATING`, `PUBLISHING`, then `SUCCEEDED`. `FAILED` and `CANCELLED` are terminal. `CANCELLING` records a best-effort request in progress.

To cancel, obtain authorization for action `cancel` on `synthetic-data/jobs/{job_id}` and call `POST /v1/jobs/{job_id}:cancel`. Cancellation is idempotent. It may stop queued or running work at a safe checkpoint, but it cannot delete or mutate an Artifact whose publication already committed.

Do not treat cancellation as revocation of an already published dataset. Retention and downstream authorization are separate concerns.
