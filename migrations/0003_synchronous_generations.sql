DROP TABLE IF EXISTS juntai_synthetic_data.worker_cleanup_evidence;
DROP TABLE IF EXISTS juntai_synthetic_data.worker_result_inbox;
DROP TABLE IF EXISTS juntai_synthetic_data.worker_outbox;
DROP TABLE IF EXISTS juntai_synthetic_data.job_attempts;
DROP TABLE IF EXISTS juntai_synthetic_data.job_transitions;
DROP TABLE IF EXISTS juntai_synthetic_data.jobs;

CREATE TABLE juntai_synthetic_data.generations (
    tenant_id          varchar(128) NOT NULL,
    generation_id      varchar(64)  NOT NULL,
    idempotency_key    varchar(200) NOT NULL,
    request_digest     char(71)     NOT NULL,
    contract_digest    char(71)     NOT NULL,
    request_json       jsonb        NOT NULL,
    seed               varchar(256) NOT NULL,
    provider_class     varchar(64)  NOT NULL,
    provider_id        varchar(128) NOT NULL,
    provider_version   varchar(64)  NOT NULL,
    policy_digest      char(71)     NOT NULL,
    state              varchar(16)  NOT NULL,
    data_digest        char(71)     NOT NULL,
    record_count       integer      NOT NULL,
    byte_count         bigint       NOT NULL,
    destinations_json  jsonb        NOT NULL,
    created_at         timestamptz  NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at         timestamptz,
    PRIMARY KEY (tenant_id, generation_id),
    UNIQUE (tenant_id, idempotency_key),
    CHECK (state IN ('COMMITTED', 'DELETED')),
    CHECK ((state = 'COMMITTED' AND deleted_at IS NULL)
        OR (state = 'DELETED' AND deleted_at IS NOT NULL)),
    CHECK (record_count > 0 AND record_count <= 1000000),
    CHECK (byte_count > 0 AND byte_count <= 1073741824),
    CHECK (octet_length(request_json::text) <= 1048576),
    CHECK (octet_length(destinations_json::text) <= 65536)
);

CREATE TABLE juntai_synthetic_data.generation_rows (
    tenant_id           varchar(128) NOT NULL,
    generation_id       varchar(64)  NOT NULL,
    insert_ordinal      integer      NOT NULL,
    record_type         varchar(63)  NOT NULL,
    destination_schema  varchar(63)  NOT NULL,
    destination_table   varchar(63)  NOT NULL,
    key_values          jsonb        NOT NULL,
    delete_rank         integer      NOT NULL,
    PRIMARY KEY (tenant_id, generation_id, insert_ordinal),
    FOREIGN KEY (tenant_id, generation_id)
        REFERENCES juntai_synthetic_data.generations (tenant_id, generation_id)
        ON DELETE RESTRICT,
    CHECK (insert_ordinal >= 0),
    CHECK (delete_rank >= 0),
    CHECK (octet_length(key_values::text) BETWEEN 2 AND 8192)
);

CREATE INDEX generations_created_idx
    ON juntai_synthetic_data.generations (tenant_id, created_at, generation_id);

CREATE INDEX generation_rows_delete_idx
    ON juntai_synthetic_data.generation_rows (
        tenant_id, generation_id, delete_rank, insert_ordinal DESC
    );

ALTER TABLE juntai_synthetic_data.generations ENABLE ROW LEVEL SECURITY;
ALTER TABLE juntai_synthetic_data.generation_rows ENABLE ROW LEVEL SECURITY;
ALTER TABLE juntai_synthetic_data.generations FORCE ROW LEVEL SECURITY;
ALTER TABLE juntai_synthetic_data.generation_rows FORCE ROW LEVEL SECURITY;

CREATE POLICY generations_tenant_isolation ON juntai_synthetic_data.generations
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

CREATE POLICY generation_rows_tenant_isolation ON juntai_synthetic_data.generation_rows
    USING (tenant_id = current_setting('juntai.tenant_id', true))
    WITH CHECK (tenant_id = current_setting('juntai.tenant_id', true));

COMMENT ON TABLE juntai_synthetic_data.generations IS
    'Bounded synchronous generation identity, idempotency, and committed/deleted evidence.';

COMMENT ON TABLE juntai_synthetic_data.generation_rows IS
    'Exact generated application-row keys retained solely for tenant-scoped deletion and audit.';
