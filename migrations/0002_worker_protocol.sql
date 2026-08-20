ALTER TABLE juntai_synthetic_data.jobs
    ADD COLUMN IF NOT EXISTS cancel_sequence integer NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS active_attempt_id varchar(96),
    ADD COLUMN IF NOT EXISTS active_attempt_number integer NOT NULL DEFAULT 0;

ALTER TABLE juntai_synthetic_data.jobs
    ADD CONSTRAINT jobs_cancel_sequence_nonnegative CHECK (cancel_sequence >= 0),
    ADD CONSTRAINT jobs_attempt_number_nonnegative CHECK (active_attempt_number >= 0);

CREATE TABLE juntai_synthetic_data.job_attempts (
    tenant_id             varchar(128) NOT NULL,
    job_id                varchar(64)  NOT NULL,
    attempt_id            varchar(96)  NOT NULL,
    attempt_number        integer      NOT NULL,
    input_artifact_json   jsonb        NOT NULL,
    worker_image_digest   char(71)     NOT NULL,
    protocol_version      varchar(64)  NOT NULL,
    status                varchar(32)  NOT NULL,
    execution_lease_id    varchar(200),
    last_event_sequence   integer      NOT NULL DEFAULT -1,
    created_at            timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at            timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, job_id, attempt_id),
    UNIQUE (tenant_id, job_id, attempt_number),
    FOREIGN KEY (tenant_id, job_id)
        REFERENCES juntai_synthetic_data.jobs (tenant_id, job_id)
        ON DELETE RESTRICT,
    CHECK (attempt_number >= 1),
    CHECK (protocol_version = 'juntai.synthetic.worker/v1'),
    CHECK (octet_length(input_artifact_json::text) <= 65536)
);

CREATE TABLE juntai_synthetic_data.worker_outbox (
    tenant_id        varchar(128) NOT NULL,
    job_id           varchar(64)  NOT NULL,
    attempt_id       varchar(96)  NOT NULL,
    channel          varchar(64)  NOT NULL,
    message_id       varchar(200) NOT NULL PRIMARY KEY,
    content_digest   char(71)     NOT NULL,
    canonical_bytes  bytea        NOT NULL,
    sequence         integer      NOT NULL,
    published_at     timestamptz,
    broker_message_id varchar(200),
    created_at       timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tenant_id, job_id, attempt_id)
        REFERENCES juntai_synthetic_data.job_attempts (tenant_id, job_id, attempt_id)
        ON DELETE RESTRICT,
    CHECK (channel IN ('synthetic.worker.dispatch.v1', 'synthetic.worker.control.v1')),
    CHECK (sequence >= 0),
    CHECK (octet_length(canonical_bytes) BETWEEN 1 AND 1048576)
);

CREATE TABLE juntai_synthetic_data.worker_result_inbox (
    tenant_id        varchar(128) NOT NULL,
    job_id           varchar(64)  NOT NULL,
    attempt_id       varchar(96)  NOT NULL,
    event_id         varchar(200) NOT NULL PRIMARY KEY,
    content_digest   char(71)     NOT NULL,
    event_type       varchar(16)  NOT NULL,
    canonical_bytes  bytea        NOT NULL,
    disposition      varchar(32)  NOT NULL,
    committed_at     timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (tenant_id, job_id, attempt_id)
        REFERENCES juntai_synthetic_data.job_attempts (tenant_id, job_id, attempt_id)
        ON DELETE RESTRICT,
    CHECK (event_type IN ('STARTED', 'STAGE', 'TERMINAL')),
    CHECK (octet_length(canonical_bytes) BETWEEN 1 AND 1048576)
);

CREATE TABLE juntai_synthetic_data.worker_cleanup_evidence (
    tenant_id       varchar(128) NOT NULL,
    job_id          varchar(64)  NOT NULL,
    attempt_id      varchar(96)  NOT NULL,
    evidence_id     varchar(200) NOT NULL,
    reason_code     varchar(64)  NOT NULL,
    artifact_json   jsonb        NOT NULL,
    recorded_at     timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tenant_id, job_id, attempt_id, evidence_id),
    FOREIGN KEY (tenant_id, job_id, attempt_id)
        REFERENCES juntai_synthetic_data.job_attempts (tenant_id, job_id, attempt_id)
        ON DELETE RESTRICT,
    CHECK (octet_length(artifact_json::text) <= 65536)
);

CREATE INDEX worker_outbox_pending_idx
    ON juntai_synthetic_data.worker_outbox (created_at, message_id)
    WHERE published_at IS NULL;

CREATE INDEX worker_attempt_reconcile_idx
    ON juntai_synthetic_data.job_attempts (status, updated_at, attempt_id);

ALTER TABLE juntai_synthetic_data.job_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE juntai_synthetic_data.worker_outbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE juntai_synthetic_data.worker_result_inbox ENABLE ROW LEVEL SECURITY;
ALTER TABLE juntai_synthetic_data.worker_cleanup_evidence ENABLE ROW LEVEL SECURITY;

CREATE POLICY attempts_tenant_isolation ON juntai_synthetic_data.job_attempts
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

CREATE POLICY worker_outbox_tenant_isolation ON juntai_synthetic_data.worker_outbox
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

CREATE POLICY worker_inbox_tenant_isolation ON juntai_synthetic_data.worker_result_inbox
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

CREATE POLICY cleanup_evidence_tenant_isolation
    ON juntai_synthetic_data.worker_cleanup_evidence
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

COMMENT ON TABLE juntai_synthetic_data.worker_outbox IS
    'Canonical SWP dispatch/control envelopes; relay state only, never generated dataset bytes.';

COMMENT ON TABLE juntai_synthetic_data.worker_result_inbox IS
    'Idempotent SWP result identities committed atomically with job metadata transitions.';
