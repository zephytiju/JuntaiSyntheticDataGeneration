# Stable failures and recovery

- `CONTRACT_INVALID`: correct the bounded generation or destination mapping.
- `PROVIDER_UNSUPPORTED`: choose an installed in-process provider with compatible requirements.
- `POLICY_DENIED`: use an authorized test-fleet identity and permitted data classification.
- `DESTINATION_FORBIDDEN`: KingbaseES denied the service role or tenant context; correct the
  test-fleet database binding, grants, or RLS.
- `DESTINATION_INVALID`: correct a malformed destination object or a schema, table, column, type,
  default, key, relation, or constraint rejected by KingbaseES.
- `DESTINATION_CONFLICT`: change the seed/rules or remove the conflicting application key. The
  failed transaction wrote nothing.
- `OUTPUT_LIMIT_EXCEEDED`: reduce record or byte bounds.
- `IDEMPOTENCY_KEY_REUSED`: replay the original identical content or choose a new key.
- `DEPENDENCY_UNAVAILABLE`: retry the identical request after the database dependency recovers.
- `GENERATION_NOT_FOUND`: verify tenant authority and the exact generation ID.
- `DELETE_CONFLICT`: restore/reconcile externally changed rows before retrying exact deletion. The
  failed deletion rolled back in full.

Never recover by supplying database credentials in the API, changing a request under an existing
idempotency key, issuing raw SQL, or deleting rows outside the exact ledger.
