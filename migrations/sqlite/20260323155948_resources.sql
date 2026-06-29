/* ------------------------------ */

CREATE TABLE resources (
    resource_id            CHAR(36) PRIMARY KEY NOT NULL,
    account_id             VARCHAR(100) NOT NULL,
    resource_schema        TEXT NOT NULL,
    resource_name          VARCHAR(200) NOT NULL,
    description            TEXT NULL,
    labels                 JSONB NOT NULL,
    annotations            JSONB NOT NULL,

    spec                   JSONB NOT NULL,
    status                 JSONB NULL,

    generation             BIGINT NOT NULL,

    created_at             TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,
    deleted_at             TIMESTAMPTZ NULL,
    last_reconciled_at     TIMESTAMPTZ NULL,
    last_event_id          BIGINT NULL,

    UNIQUE (account_id, resource_schema, resource_name)
);

CREATE INDEX idx_resources_account_schema_name
    ON resources (account_id, resource_schema, resource_name);

CREATE INDEX idx_resources_account_schema_updated_at
    ON resources (account_id, resource_schema, updated_at DESC);

CREATE INDEX idx_resources_summary_projection
    ON resources (
        account_id,
        resource_schema,
        json_extract(status, '$.phase')
    )
    WHERE deleted_at IS NULL;

/* ------------------------------ */

CREATE TABLE resource_events (
    event_id               INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    resource_id            CHAR(36) NOT NULL,
    resource_schema        TEXT NOT NULL,

    event_time             TIMESTAMPTZ NOT NULL,
    event_type             VARCHAR(100) NOT NULL,
    event_payload          JSONB NOT NULL,

    CONSTRAINT fk_resource_events_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id)
);

CREATE INDEX idx_resource_events_resource_id_event_id
    ON resource_events (resource_id, event_id);

CREATE INDEX idx_resource_events_schema_event_id
    ON resource_events (resource_schema, event_id);

/* ------------------------------ */
