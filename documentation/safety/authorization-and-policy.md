# Authorization and policy

Every operation requires a bearer token with exact audience `juntai.synthetic-data.api`, a verified
human or live delegated-agent caller, the `synthetic-data:generations` scope, and authorization for
the tenant-scoped generation resource and action. Tenant identity comes only from the verified IAM
context, never from a request field or caller-controlled header.

The runtime imports the immutable `juntai-iam==1.1.0` and `juntai-iam-contracts==1.1.1` releases and
verifies the contract manifest before constructing authorization. It does not recreate token,
peer-principal, delegation, or policy semantics.

Only `synthetic` and `internal` classifications are accepted. The service is test-fleet-only and
must not be admitted to a production application fleet. Authenticated internal callers are
authoritative for logical destinations; normal database grants and RLS still apply when KingbaseES
executes the transaction. Synthetic maintains no separate destination policy.
