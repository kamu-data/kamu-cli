/* ------------------------------ */

CREATE TABLE resources (
    resource_uid           CHAR(36) PRIMARY KEY NOT NULL,
    account_id             VARCHAR(100) NOT NULL,
    resource_kind          VARCHAR(100) NOT NULL,
    api_version            VARCHAR(100) NOT NULL,
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

    UNIQUE (account_id, resource_kind, resource_name)
);

CREATE INDEX idx_resources_account_kind_name
    ON resources (account_id, resource_kind, resource_name);

CREATE INDEX idx_resources_account_kind_updated_at
    ON resources (account_id, resource_kind, updated_at DESC);

/* ------------------------------ */

CREATE TABLE resource_events (
    event_id               INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    resource_uid           CHAR(36) NOT NULL,
    resource_kind          VARCHAR(100) NOT NULL,

    event_time             TIMESTAMPTZ NOT NULL,
    event_type             VARCHAR(100) NOT NULL,
    event_payload          JSONB NOT NULL,

    CONSTRAINT fk_resource_events_resource_uid
        FOREIGN KEY (resource_uid) REFERENCES resources(resource_uid)
);

CREATE INDEX idx_resource_events_resource_uid_event_id
    ON resource_events (resource_uid, event_id);

CREATE INDEX idx_resource_events_kind_event_id
    ON resource_events (resource_kind, event_id);

/* ------------------------------ */
