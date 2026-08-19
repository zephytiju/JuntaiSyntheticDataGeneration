# Exact HTTP API and lifecycle

This bundle binds the exact 1.2.0 OpenAPI digest
`sha256:b085ddac0d23bdf2ea307f0055685d55865a261804a647ef2d7de9da2f7bbacf`
copied from reviewed source `1e9105dabe022a58047ed2dd83a7353478f925aa`.

| Method and path | Operation ID | Purpose |
| --- | --- | --- |
| `POST /v1/jobs/` | `syntheticData.createJob` | Create or replay an asynchronous job. |
| `GET /v1/jobs/{job_id}` | `syntheticData.getJob` | Read bounded status and evidence. |
| `POST /v1/jobs/{job_id}:cancel` | `syntheticData.cancelJob` | Request best-effort cancellation. |
| `GET /v1/jobs/{job_id}/result` | `syntheticData.getJobResult` | Read one exact immutable result after success. |

All response models reject unknown fields. Digests use lowercase `sha256:` values. `JobStatus` carries `job_id`, `state`, `stage`, `request_digest`, version, timestamps, optional quota, and optional failure. `JobResult` carries one Artifact reference plus exact dataset and provenance facts.

Every operation uses the reviewed HTTP bearer scheme. Tokens must have the exact audience
`juntai.synthetic-data.api` and scope `synthetic-data:jobs`. The service authenticates a human or
delegated identity, takes tenant authority only from the verified token, and authorizes `create`,
`read`, or `cancel` against the exact job resource. Caller-supplied tenant identifiers are not
authoritative; missing, invalid, wrong-audience, or unauthorized credentials fail closed.

Creating a job atomically records a SWP attempt and canonical dispatch outbox record after the
immutable input Artifact is published. Worker progress is visible only after the API coordinator
authenticates the pinned Platform executor, verifies the canonical envelope and exact Artifacts,
and commits the event with the job transition in KES.

This release has no MCP Tools. Its exact FuseAPI descriptor is still packaged and verified so consumers fail closed if later builds add or change a runtime surface.
