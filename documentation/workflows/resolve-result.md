# Resolve a successful dataset result

1. Confirm `GET /v1/jobs/{job_id}` reports `SUCCEEDED`.
2. Obtain a bearer token with audience `juntai.synthetic-data.api` and authorization for action
   `read` on `synthetic-data/jobs/{job_id}`.
3. Call `GET /v1/jobs/{job_id}/result`.
4. Verify the exact Artifact reference, dataset manifest digest, format, compression, counts, seed, validator result, and provenance.
5. Resolve and download bytes through the approved backend Artifact client using `artifact_id`, `version_id`, and `digest` together.

The API does not return payload bytes, OCI credentials, target-store credentials, or a mutable download URL. `SUCCEEDED` means both direct OCI publication and generic metadata registration completed.

Preserve provenance with the dataset reference. It binds request and contract digests, provider and model identity, seed, policy, quota reservation, worker image, validator evidence, logical dataset digest, Artifact digest, counts, shards, and timestamps.

`SUCCEEDED` is committed only after the API coordinator verifies the authenticated Platform executor,
exact tenant/attempt/input/image bindings, required SWP capabilities, dataset Artifact, and execution
evidence Artifact. Replayed event IDs with the same digest are idempotent; a different digest is a
conflict and never changes the result.
