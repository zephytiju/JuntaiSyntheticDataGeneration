ALTER TABLE juntai_synthetic_data.worker_outbox
    ADD COLUMN IF NOT EXISTS platform_publication_id varchar(200),
    ADD COLUMN IF NOT EXISTS lease_owner varchar(128),
    ADD COLUMN IF NOT EXISTS lease_token varchar(64),
    ADD COLUMN IF NOT EXISTS lease_expires_at timestamptz,
    ADD COLUMN IF NOT EXISTS publish_attempts integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS next_attempt_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ADD COLUMN IF NOT EXISTS last_error_code varchar(64),
    ADD COLUMN IF NOT EXISTS last_error_at timestamptz,
    ADD COLUMN IF NOT EXISTS updated_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP;

ALTER TABLE juntai_synthetic_data.worker_outbox
    ADD CONSTRAINT worker_outbox_publish_attempts_nonnegative
        CHECK (publish_attempts >= 0),
    ADD CONSTRAINT worker_outbox_lease_complete
        CHECK (
            (lease_owner IS NULL AND lease_token IS NULL AND lease_expires_at IS NULL)
            OR
            (lease_owner IS NOT NULL AND lease_token IS NOT NULL AND lease_expires_at IS NOT NULL)
        );

CREATE TABLE juntai_synthetic_data.worker_dead_letter_inbox (
    tenant_id                varchar(128) NOT NULL,
    job_id                   varchar(64)  NOT NULL,
    attempt_id               varchar(96)  NOT NULL,
    dead_letter_id           varchar(200) NOT NULL PRIMARY KEY,
    original_channel         varchar(64)  NOT NULL,
    message_id               varchar(200) NOT NULL,
    content_digest           char(71)     NOT NULL,
    original_content_digest  char(71)     NOT NULL,
    record_digest            char(71)     NOT NULL,
    canonical_bytes          bytea        NOT NULL,
    delivery_count           integer      NOT NULL,
    producer_namespace       varchar(200),
    producer_service_account varchar(200),
    terminal_reason_code     varchar(64)  NOT NULL,
    ledger_evidence_id       varchar(200) NOT NULL,
    event_id                 varchar(200),
    disposition              varchar(64)  NOT NULL,
    committed_at             timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tenant_id, job_id, attempt_id)
        REFERENCES juntai_synthetic_data.job_attempts (tenant_id, job_id, attempt_id)
        ON DELETE RESTRICT,
    CHECK (
        original_channel IN (
            'synthetic.worker.dispatch.v1',
            'synthetic.worker.control.v1',
            'synthetic.worker.result.v1'
        )
    ),
    CHECK (delivery_count = 5),
    CHECK (octet_length(canonical_bytes) BETWEEN 1 AND 1048576),
    CHECK (
        (producer_namespace IS NULL AND producer_service_account IS NULL)
        OR
        (producer_namespace IS NOT NULL AND producer_service_account IS NOT NULL)
    )
);

CREATE INDEX worker_outbox_relay_ready_idx
    ON juntai_synthetic_data.worker_outbox (next_attempt_at, created_at, message_id)
    WHERE published_at IS NULL;

CREATE INDEX worker_outbox_lease_expiry_idx
    ON juntai_synthetic_data.worker_outbox (lease_expires_at, message_id)
    WHERE published_at IS NULL AND lease_expires_at IS NOT NULL;

CREATE INDEX worker_dead_letter_reconcile_idx
    ON juntai_synthetic_data.worker_dead_letter_inbox
        (tenant_id, job_id, committed_at, dead_letter_id);

ALTER TABLE juntai_synthetic_data.worker_dead_letter_inbox ENABLE ROW LEVEL SECURITY;

CREATE POLICY worker_dead_letter_tenant_isolation
    ON juntai_synthetic_data.worker_dead_letter_inbox
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

COMMENT ON TABLE juntai_synthetic_data.worker_dead_letter_inbox IS
    'Idempotent SWP delivery-exhaustion evidence committed with service-owned job state.';

COMMENT ON COLUMN juntai_synthetic_data.worker_outbox.canonical_bytes IS
    'Exact immutable SWP bytes published unchanged by the API-side relay.';
