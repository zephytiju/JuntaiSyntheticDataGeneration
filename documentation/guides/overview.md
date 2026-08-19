# Synthetic Data Generation

Synthetic Data Generation 1.2.0 creates bounded, deterministic candidate datasets as immutable
Artifacts. The service owns asynchronous jobs, structural contract validation, provider selection,
privacy policy, quota reservation, the `juntai.synthetic.worker/v1` protocol, isolated generation,
optional exact validator execution, publication, and provenance.

The output is an immutable dataset Artifact. It is not imported application data and does not authorize a write to any target datastore. A separate authorized owner decides whether and how to ingest it.

## Supported public flow

- Create a job with `POST /v1/jobs/` and a bounded `Idempotency-Key`.
- Poll `GET /v1/jobs/{job_id}` until a terminal state.
- Request best-effort cancellation with `POST /v1/jobs/{job_id}:cancel` before publication commits.
- Read an exact successful result with `GET /v1/jobs/{job_id}/result`.
- Resolve dataset bytes only through the Artifact client using the returned exact reference.

## Ownership boundary

The service accepts generic record families, primitive field types, distributions, relations, limits, output format, policy, provider requirements, seed, and an optional exact validator Artifact. It does not interpret product-domain meaning, receive target-store credentials, ingest results, deploy previews, or promote data.

The API coordinator alone writes Synthetic job metadata in KES. Platform owns the durable SWP
dispatch, control, result, and dead-letter queues and the generic executor sidecar. The worker owns
generation and immutable Artifact publication behind one canonical framed Unix socket. It has no
KES, queue, Synthetic API, or Kubernetes API capability.

The pinned FuseAPI 2.0.0 MCP descriptor contains no Tools because this release exposes HTTP only.
This bundle therefore publishes MCP Resources for the same reviewed documentation units and does
not invent MCP Tools or Prompts.

## Exact release binding

- Service source: `1e9105dabe022a58047ed2dd83a7353478f925aa`.
- OpenAPI: `sha256:b085ddac0d23bdf2ea307f0055685d55865a261804a647ef2d7de9da2f7bbacf`.
- MCP descriptor: `sha256:8ce1f760e84279745ef63396cc110ffa68df31a1e1ba836994454801fa7946b4`.
- SWP schema: `contracts/worker-protocol/swp.v1.schema.json` in the service release.
- Documentation packager and renderers: `JuntaiDocumentationCapabilityBundle` `v1.0.0`
