# Exact HTTP API and lifecycle

This bundle binds the exact 1.2.0 OpenAPI digest
`sha256:26200a846179369af5c7f86e248f8eb1fa8085d62ddde994812ba348e68c93a8`
copied from reviewed source `a7511342311e84baf9f65045b8c9e72d4b3f23bd`.

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
