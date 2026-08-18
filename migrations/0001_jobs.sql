CREATE SCHEMA IF NOT EXISTS juntai_synthetic_data;

CREATE TABLE IF NOT EXISTS juntai_synthetic_data.jobs (
    job_id                 varchar(64)  NOT NULL,
    tenant_id              varchar(128) NOT NULL,
    idempotency_key        varchar(200) NOT NULL,
    request_digest         char(71)     NOT NULL,
    request_json           jsonb        NOT NULL,
    state                  varchar(32)  NOT NULL,
    version                bigint       NOT NULL,
    created_at             timestamptz  NOT NULL,
    updated_at             timestamptz  NOT NULL,
    quota_json             jsonb,
    provider_id            varchar(128),
    worker_image_digest    char(71),
    failure_json           jsonb,
    result_json            jsonb,
    cancellation_requested boolean      NOT NULL DEFAULT false,
    PRIMARY KEY (tenant_id, job_id),
    UNIQUE (tenant_id, idempotency_key),
    CHECK (state IN ('ACCEPTED','POLICY_CHECK','QUEUED','RUNNING','VALIDATING',
                     'PUBLISHING','SUCCEEDED','FAILED','CANCELLING','CANCELLED')),
    CHECK (octet_length(request_json::text) <= 1048576),
    CHECK (result_json IS NULL OR octet_length(result_json::text) <= 262144),
    CHECK (failure_json IS NULL OR octet_length(failure_json::text) <= 65536)
);

CREATE TABLE IF NOT EXISTS juntai_synthetic_data.job_transitions (
    tenant_id   varchar(128) NOT NULL,
    job_id      varchar(64)  NOT NULL,
    sequence    integer      NOT NULL,
    from_state  varchar(32),
    to_state    varchar(32)  NOT NULL,
    occurred_at timestamptz  NOT NULL,
    reason      varchar(500),
    PRIMARY KEY (tenant_id, job_id, sequence),
    FOREIGN KEY (tenant_id, job_id)
        REFERENCES juntai_synthetic_data.jobs (tenant_id, job_id)
        ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS jobs_runnable_idx
    ON juntai_synthetic_data.jobs (state, created_at, job_id)
    WHERE state NOT IN ('SUCCEEDED','FAILED','CANCELLED');

ALTER TABLE juntai_synthetic_data.jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE juntai_synthetic_data.job_transitions ENABLE ROW LEVEL SECURITY;

CREATE POLICY jobs_tenant_isolation ON juntai_synthetic_data.jobs
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

CREATE POLICY transitions_tenant_isolation ON juntai_synthetic_data.job_transitions
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

COMMENT ON TABLE juntai_synthetic_data.jobs IS
    'Bounded synthetic-data job metadata only; generated dataset rows and files are prohibited.';
