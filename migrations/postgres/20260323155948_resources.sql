/* ------------------------------ */

CREATE TABLE resources (
    resource_id            UUID PRIMARY KEY,
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
    last_event_id          BIGINT NULL
);

-- Resource names are case-insensitive per the ODF `ResourceName` grammar, so
-- uniqueness and lookups must be enforced/optimized on the folded form.
CREATE UNIQUE INDEX idx_resources_account_schema_name_ci
    ON resources (account_id, resource_schema, LOWER(resource_name));

CREATE INDEX idx_resources_account_schema_updated_at
    ON resources (account_id, resource_schema, updated_at DESC);

CREATE INDEX idx_resources_summary_projection
    ON resources (
        account_id,
        resource_schema,
        ((status ->> 'phase'))
    )
    WHERE deleted_at IS NULL;

/* ------------------------------ */

CREATE SEQUENCE resource_event_id_seq AS BIGINT;

CREATE TABLE resource_events (
    event_id               BIGINT PRIMARY KEY DEFAULT nextval('resource_event_id_seq'),
    resource_id            UUID NOT NULL,
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
