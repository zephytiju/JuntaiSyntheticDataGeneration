# Monitor or cancel a job

Poll `GET /v1/jobs/{job_id}` with bounded backoff. The response returns state, stage, timestamps, request digest, optimistic-concurrency version, bounded quota details, and a stable failure when present.

The normal sequence is `ACCEPTED`, `POLICY_CHECK`, `QUEUED`, `RUNNING`, `VALIDATING`, `PUBLISHING`, then `SUCCEEDED`. `FAILED` and `CANCELLED` are terminal. `CANCELLING` records a best-effort request in progress.

To cancel, obtain a correct-audience bearer token and authorization for action `cancel` on
`synthetic-data/jobs/{job_id}` and call `POST /v1/jobs/{job_id}:cancel`. Cancellation is idempotent.
The API increments a monotonic cancel sequence and atomically records the control outbox. The
executor forwards the highest sequence over the authenticated fenced stream, allows the specified grace interval,
then terminates the worker if necessary. A success arriving after cancellation committed is not
associated with the job. Cancellation cannot delete or mutate an Artifact whose publication already
committed.

Do not treat cancellation as revocation of an already published dataset. Retention and downstream authorization are separate concerns.
