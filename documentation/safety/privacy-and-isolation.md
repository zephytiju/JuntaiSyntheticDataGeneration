# Privacy, isolation, and immutable output

Prefer schema-only generation. Do not send secrets, credentials, protected payloads, raw production rows, target-store bindings, or unrestricted source examples.

Workers use digest-pinned images, read-only roots, bounded CPU, memory, process, time, and storage, denied-by-default network, least-privileged Artifact publication identity, and ephemeral workspaces. Validator isolation is stricter and has no network.

Candidate files remain bounded by the request and quota. The worker creates canonical shards and a manifest, publishes bytes directly through the Artifact SDK to OCI, verifies digests and media types, registers generic metadata, records the exact reference, and removes temporary data.

Provenance must not contain credentials or target-store authority. Telemetry uses bounded identifiers and digests, not dataset payloads or authored documentation bodies.

Every successful job points to one immutable Artifact. Never replace an exact digest with `latest`, a channel, a short-lived URL, or a local path.
