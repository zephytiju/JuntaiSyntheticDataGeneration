# Synthetic Data Generation

Synthetic Data Generation creates bounded, deterministic candidate datasets as immutable Artifacts. The service owns asynchronous jobs, structural contract validation, provider selection, privacy policy, quota reservation, isolated generation, optional exact validator execution, publication, and provenance.

The output is an immutable dataset Artifact. It is not imported application data and does not authorize a write to any target datastore. A separate authorized owner decides whether and how to ingest it.

## Supported public flow

- Create a job with `POST /v1/jobs/` and a bounded `Idempotency-Key`.
- Poll `GET /v1/jobs/{job_id}` until a terminal state.
- Request best-effort cancellation with `POST /v1/jobs/{job_id}:cancel` before publication commits.
- Read an exact successful result with `GET /v1/jobs/{job_id}/result`.
- Resolve dataset bytes only through the Artifact client using the returned exact reference.

## Ownership boundary

The service accepts generic record families, primitive field types, distributions, relations, limits, output format, policy, provider requirements, seed, and an optional exact validator Artifact. It does not interpret product-domain meaning, receive target-store credentials, ingest results, deploy previews, or promote data.

The pinned FuseAPI 2.0.0 MCP descriptor for source `2a4bd9ec4d33c8a7ef2d0f5ca1ee9155208ffa5b` contains no Tools because this release exposes HTTP only. This bundle therefore publishes MCP Resources for the same reviewed documentation units and does not invent MCP Tools or Prompts.

## Exact release binding

- Service source: `2a4bd9ec4d33c8a7ef2d0f5ca1ee9155208ffa5b`
- OpenAPI: `sha256:a1b68d7f8a76807b55e8707c49b88679e9a2ef288bc5d8d9966dd1fd4cafab60`
- MCP descriptor: `sha256:5304fcfce8234a2428f83f8785dd2d8d6b34f32ff67db3df5464829084301e9a`
- Documentation packager and renderers: `JuntaiDocumentationCapabilityBundle` `v1.0.0`
