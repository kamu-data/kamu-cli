/* ------------------------------ */

CREATE TABLE resources (
    resource_id            CHAR(36) PRIMARY KEY NOT NULL,
    account_id             VARCHAR(100) NOT NULL,
    resource_kind          VARCHAR(100) NOT NULL,
    api_version            VARCHAR(100) NOT NULL,
    resource_name          VARCHAR(200) NOT NULL,

    spec                   JSONB NOT NULL,
    status                 JSONB NULL,

    generation             BIGINT NOT NULL,
    observed_generation    BIGINT NULL,
    phase                  VARCHAR(50) NULL,

    created_at             TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,
    last_reconciled_at     TIMESTAMPTZ NULL,
    last_event_id          BIGINT NULL,

    UNIQUE (account_id, resource_kind, resource_name)
);

CREATE INDEX idx_resources_account_kind_name
    ON resources (account_id, resource_kind, resource_name);

CREATE INDEX idx_resources_account_kind_updated_at
    ON resources (account_id, resource_kind, updated_at DESC);

CREATE INDEX idx_resources_account_kind_phase
    ON resources (account_id, resource_kind, phase);

CREATE INDEX idx_resources_kind_phase
    ON resources (resource_kind, phase);

/* ------------------------------ */

CREATE TABLE resource_events (
    event_id               INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    resource_id            CHAR(36) NOT NULL,
    account_id             VARCHAR(100) NOT NULL,
    resource_kind          VARCHAR(100) NOT NULL,

    event_time             TIMESTAMPTZ NOT NULL,
    event_type             VARCHAR(100) NOT NULL,
    event_payload          JSONB NOT NULL,

    CONSTRAINT fk_resource_events_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id)
);

CREATE INDEX idx_resource_events_resource_id_event_id
    ON resource_events (resource_id, event_id);

CREATE INDEX idx_resource_events_kind_event_id
    ON resource_events (resource_kind, event_id);

CREATE INDEX idx_resource_events_account_kind_event_id
    ON resource_events (account_id, resource_kind, event_id);

/* ------------------------------ */
