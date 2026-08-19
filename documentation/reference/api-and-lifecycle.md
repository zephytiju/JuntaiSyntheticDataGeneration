# Exact HTTP API and lifecycle

This bundle binds the exact OpenAPI digest `sha256:a1b68d7f8a76807b55e8707c49b88679e9a2ef288bc5d8d9966dd1fd4cafab60`.

| Method and path | Operation ID | Purpose |
| --- | --- | --- |
| `POST /v1/jobs/` | `syntheticData.createJob` | Create or replay an asynchronous job. |
| `GET /v1/jobs/{job_id}` | `syntheticData.getJob` | Read bounded status and evidence. |
| `POST /v1/jobs/{job_id}:cancel` | `syntheticData.cancelJob` | Request best-effort cancellation. |
| `GET /v1/jobs/{job_id}/result` | `syntheticData.getJobResult` | Read one exact immutable result after success. |

All response models reject unknown fields. Digests use lowercase `sha256:` values. `JobStatus` carries `job_id`, `state`, `stage`, `request_digest`, version, timestamps, optional quota, and optional failure. `JobResult` carries one Artifact reference plus exact dataset and provenance facts.

The service authenticates a human or delegated identity, takes tenant authority only from the verified token, and authorizes `create`, `read`, or `cancel` against the job resource. Caller-supplied tenant identifiers are not authoritative.

This release has no MCP Tools. Its exact FuseAPI descriptor is still packaged and verified so consumers fail closed if later builds add or change a runtime surface.
