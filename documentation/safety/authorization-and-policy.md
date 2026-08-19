# Authorization, policy, and approvals

Every operation requires a bearer access token with exact audience `juntai.synthetic-data.api`,
verified human or delegated identity, the `synthetic-data:jobs` scope, and an authorization decision
for the exact resource and action. Service tokens without the required caller context are not a
substitute.

The default policy permits `synthetic` and `internal` classifications. It denies `confidential` and `restricted` requests. `source_examples` defaults to `none`; `minimized` requires an explicit `authorization_reference`. Validators must declare deterministic behavior.

Quota is reserved before execution for concurrent jobs, daily jobs, records, bytes, compute time, provider class, model tokens, and retained evidence. A caller must reduce bounds or wait for the reported quota window rather than bypass policy.

Creating a job authorizes bounded generation only. It does not authorize ingestion, mutation of an authoritative datastore, preview deployment, or production promotion. Those decisions remain with the separately authorized owner.

Cancellation is best effort before publication commit. Confirm the intended `job_id`; cancelling one job cannot revoke or rewrite an already published Artifact.

SWP result messages require the pinned Platform executor workload identity and the job tenant,
attempt, worker image, input digest, and required protocol capabilities. Transport identity,
`producerWorkload`, and content digest must agree. The API rejects spoofed or stale results and is
the sole component permitted to change job state in KES.
