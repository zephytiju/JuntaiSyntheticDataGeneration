# Exact validator Artifact reference

A validator is optional. When supplied, it must contain immutable `artifact_id`, `version_id`, lowercase SHA-256 digest, media type, runtime, documented entry point, protocol versions, resource limits, and a deterministic declaration.

The service resolves the exact Artifact through `juntai-artifact-client`, verifies the returned manifest digest, and executes it in a no-network, read-only, non-root sandbox with bounded CPU, memory, time, and findings.

The entry point format is `module.path:function`. Supported runtimes are `python` and `wasm`. The validator input and output protocols are `juntai.synthetic-data.validator-input/v1` and `juntai.synthetic-data.validator-output/v1`.

A validator must not start a server, database connection, engine, background thread, or network client. It must not mutate candidate data. Digest drift, side effects, timeout, unbounded findings, or a failed result prevents dataset publication.

Never submit a mutable tag, unpinned package name, local filesystem path, or credential as a validator reference.
