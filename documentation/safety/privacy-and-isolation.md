# Privacy, isolation, and immutable output

Prefer schema-only generation. Do not send secrets, credentials, protected payloads, raw production rows, target-store bindings, or unrestricted source examples.

Workers use digest-pinned images, read-only roots, bounded CPU, memory, process, time, and storage,
least-privileged Artifact publication identity, and ephemeral workspaces. The worker receives only
exact Artifact credentials and `/var/run/juntai-worker/swp-v1.sock`. It must not receive a KES DSN,
KES secret or mount, queue endpoint/token, Synthetic API credential, service-account token,
Kubernetes API configuration, or network route to KES, queue, Synthetic API, or Kubernetes API.
Platform allows only the reviewed Artifact Registry/OCI endpoints. Validator isolation is stricter
and has no network.

Candidate files remain bounded by the request and quota. The worker creates canonical shards and a manifest, publishes bytes directly through the Artifact SDK to OCI, verifies digests and media types, registers generic metadata, records the exact reference, and removes temporary data.

Provenance must not contain credentials or target-store authority. Telemetry uses bounded identifiers and digests, not dataset payloads or authored documentation bodies.

Every successful job points to one immutable Artifact. Never replace an exact digest with `latest`, a channel, a short-lived URL, or a local path.

The generic executor sidecar owns queue visibility, acknowledgement, renewal, retries, DLQ routing,
socket permissions, and process termination. It runs beside—but is not part of—the worker. A local
production-equivalent acceptance run places the worker on an internal network that cannot resolve
or connect to the real-KES container and fails if any forbidden configuration or mount is present.
