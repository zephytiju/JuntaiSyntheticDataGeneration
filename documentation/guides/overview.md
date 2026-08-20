# Synthetic Data Generation

Synthetic Data Generation 1.3.0 creates bounded, deterministic candidate datasets as immutable
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
dispatch, control, result, and dead-letter queues, generic delivery ledger, and executor Deployment.
The relay reaches only the executor's authenticated QueueTransport listener on the injected literal
ClusterIP:7444; it receives no Kafka or Platform-ledger credential and remote readiness must pass
before a Synthetic KES outbox lease.
Each committed claim creates a separately fenced worker Job Pod. The worker owns generation and
immutable Artifact publication behind the authenticated remote SWP stream. The Unix socket remains
local-test framing only. The worker has no
KES, queue, Synthetic API, or Kubernetes API capability.

The pinned FuseAPI 2.0.0 MCP descriptor contains no Tools because this release exposes HTTP only.
This bundle therefore publishes MCP Resources for the same reviewed documentation units and does
not invent MCP Tools or Prompts.

## Exact release binding

- Service source, image, OpenAPI, MCP descriptor, SWP schema, queue adapter, stream client, and
  documentation/capability digests are populated only by the final immutable 1.3.0 release build.
- SWP schema: `contracts/worker-protocol/swp.v1.schema.json` in the service release.
- Documentation packager and renderers: `JuntaiDocumentationCapabilityBundle` `v1.0.0`
