# Privacy, isolation, and immutable output

Prefer schema-only generation. Do not send secrets, credentials, protected payloads, raw production rows, target-store bindings, or unrestricted source examples.

Workers use digest-pinned images, read-only roots, bounded CPU, memory, process, time, and storage,
least-privileged Artifact publication identity, and ephemeral workspaces. The separate worker Job
Pod receives only exact Artifact credentials, the executor ClusterIP:7443 address and pinned CA, and
a Pod-bound token whose sole audience is `juntai-platform-swp-executor`. It must not receive a KES DSN,
KES secret or mount, queue endpoint/token, Synthetic API credential, default service-account token,
Kubernetes API configuration, or network route to KES, queue, Synthetic API, or Kubernetes API.
Platform allows only the executor, reviewed Artifact Registry/OCI, OTel, and explicitly declared
provider endpoints. DNS/world egress remains denied. Validator isolation is stricter.

Candidate files remain bounded by the request and quota. The worker creates canonical shards and a manifest, publishes bytes directly through the Artifact SDK to OCI, verifies digests and media types, registers generic metadata, records the exact reference, and removes temporary data.

Provenance must not contain credentials or target-store authority. Telemetry uses bounded identifiers and digests, not dataset payloads or authored documentation bodies.

Every successful job points to one immutable Artifact. Never replace an exact digest with `latest`, a channel, a short-lived URL, or a local path.

The generic Platform executor Deployment owns queue receipts, the durable delivery ledger,
generation fencing, retry timing, the fifth-failure DLQ transition, and per-claim Job termination.
It is a separate Pod and identity from every worker. Cilium policy-enforcement-always and namespace
default deny must be proven before a canonical frame is released. The local Unix socket is used only
by isolated framing tests and is forbidden in production rendering.

The Synthetic relay's mTLS credential identifies only its service principal for the executor
QueueTransport resource/audience on port 7444. It is not a Kafka or Platform-KES credential. Relay
egress to Kafka, Platform KES, worker Attach, Kubernetes workload management, and arbitrary network
remains denied; wrong peer identity, server name, audience/resource, channel, or operation fails
closed before a Synthetic KES lease.
