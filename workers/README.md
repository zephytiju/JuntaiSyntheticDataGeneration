# Worker execution contract

The same immutable image runs in `worker` mode. JuntaiPlatformInfrastructure must select it by
`@sha256:` digest and supplies the queue identity, deny-all network policy (except the exact
Artifact publication egress), read-only root filesystem, non-root UID, seccomp, no privilege
escalation, explicit CPU/memory/PID/time/ephemeral-storage limits, and cleanup Job.

The scheduler rejects any provider manifest without a canonical image digest or with a network
policy other than `deny-all`. Workers never receive a KES namespace, KES credential, target-store
binding, preview route, or promotion authority.
