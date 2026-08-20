# Authorization, policy, and approvals

Every operation requires a bearer access token with exact audience `juntai.synthetic-data.api`,
verified human or delegated identity, the `synthetic-data:jobs` scope, and an authorization decision
for the exact resource and action. Service tokens without the required caller context are not a
substitute.

The runtime pins `juntai-iam==1.1.0` and its exact dependency
`juntai-iam-contracts==1.1.1`. It verifies contract-manifest SHA-256
`64dafb25c54d40320347c8661960d23ba524a2d3c102d112c08c95679d12db85` before
constructing authorization. The published IAM library—not this service—owns token validation,
peer-principal identity, live delegation intersection, stable IAM failures, and policy evaluation.
A service or self-authorized agent token cannot substitute for the required human/delegated caller
context; stale epochs, mismatched audience/resource/tenant, or incomplete grant context deny without
fallback.

The default policy permits `synthetic` and `internal` classifications. It denies `confidential` and `restricted` requests. `source_examples` defaults to `none`; `minimized` requires an explicit `authorization_reference`. Validators must declare deterministic behavior.

Quota is reserved before execution for concurrent jobs, daily jobs, records, bytes, compute time, provider class, model tokens, and retained evidence. A caller must reduce bounds or wait for the reported quota window rather than bypass policy.

Creating a job authorizes bounded generation only. It does not authorize ingestion, mutation of an authoritative datastore, preview deployment, or production promotion. Those decisions remain with the separately authorized owner.

Cancellation is best effort before publication commit. Confirm the intended `job_id`; cancelling one job cannot revoke or rewrite an already published Artifact.

SWP result messages require the pinned Platform executor workload identity and the job tenant,
attempt, worker image, input digest, and required protocol capabilities. Transport identity,
`producerWorkload`, and content digest must agree. The API rejects spoofed or stale results and is
the sole component permitted to change job state in KES.
