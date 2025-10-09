/* ------------------------------ */

PRAGMA foreign_keys = OFF;

/* ------------------------------ */

-- Reset all flow-related tables to start fresh

UPDATE flows SET last_event_id = NULL;
DELETE FROM flows;
DELETE FROM flow_ids;

DROP TABLE flow_events;
DROP TABLE flow_trigger_events;
DROP TABLE flow_configuration_events;

UPDATE sqlite_sequence SET seq = 0 WHERE name = 'flow_ids';

/* ------------------------------ */

-- We will not need the old AUTOINCREMENT fields anymore

DELETE FROM sqlite_sequence WHERE name = 'flow_events';
DELETE FROM sqlite_sequence WHERE name = 'flow_configuration_events';
DELETE FROM sqlite_sequence WHERE name = 'flow_trigger_events';

/* ------------------------------ */

-- Bind all event tables to a single global counter
-- (SQLite doesn't have sequences, so we use a custom counter table with triggers)

CREATE TABLE IF NOT EXISTS flow_event_global_counter (
    name TEXT PRIMARY KEY CHECK (name = 'global'),
    val  INTEGER NOT NULL
);

-- Guarantee there is always a row to work with
INSERT OR IGNORE INTO flow_event_global_counter(name, val) VALUES ('global', 0);

/* ------------------------------ */

-- Re-create trigger events table to assign global event_id

CREATE TABLE flow_trigger_events (
    event_id INTEGER PRIMARY KEY NOT NULL,
    flow_type VARCHAR(100) NOT NULL,
    scope_data JSONB NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

-- Assign global event_id from shared counter on insert
CREATE TRIGGER fte_assign_event_id
    AFTER INSERT ON flow_trigger_events
BEGIN
    UPDATE flow_event_global_counter SET val = val + 1 WHERE name = 'global';
    UPDATE flow_trigger_events
        SET event_id = (SELECT val FROM flow_event_global_counter WHERE name = 'global')
        WHERE rowid = NEW.rowid;
END;

-- Recover indices

CREATE INDEX idx_fte_dataset_scope
    ON flow_trigger_events (flow_type, json_extract(scope_data, '$.dataset_id'))
    WHERE json_extract(scope_data, '$.type') = 'Dataset';

CREATE INDEX idx_fte_system_scope
    ON flow_trigger_events (flow_type)
    WHERE json_extract(scope_data, '$.type') = 'System';    

/* ------------------------------ */

-- Re-create configuration events table to assign global event_id

CREATE TABLE flow_configuration_events (
    event_id INTEGER PRIMARY KEY NOT NULL,
    flow_type VARCHAR(100) NOT NULL,
    scope_data JSONB NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

-- Assign global event_id from shared counter on insert
CREATE TRIGGER fce_assign_event_id
    AFTER INSERT ON flow_configuration_events
BEGIN
    UPDATE flow_event_global_counter SET val = val + 1 WHERE name = 'global';
    UPDATE flow_configuration_events
        SET event_id = (SELECT val FROM flow_event_global_counter WHERE name = 'global')
        WHERE rowid = NEW.rowid;
END;

-- Recover indices

CREATE INDEX idx_fce_dataset_scope
    ON flow_configuration_events (flow_type, json_extract(scope_data, '$.dataset_id'))
    WHERE json_extract(scope_data, '$.type') = 'Dataset';

CREATE INDEX idx_fce_system_scope
    ON flow_configuration_events (flow_type)
    WHERE json_extract(scope_data, '$.type') = 'System';

/* ------------------------------ */

-- Re-create flow events table to assign global event_id

CREATE TABLE flow_events
(
    event_id      INTEGER PRIMARY KEY NOT NULL,
    flow_id       BIGINT NOT NULL REFERENCES flows(flow_id),
    event_type    VARCHAR(50) NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

-- Assign global event_id from shared counter on insert
CREATE TRIGGER fe_assign_event_id
    AFTER INSERT ON flow_events
BEGIN
    UPDATE flow_event_global_counter SET val = val + 1 WHERE name = 'global';
    UPDATE flow_events
        SET event_id = (SELECT val FROM flow_event_global_counter WHERE name = 'global')
        WHERE rowid = NEW.rowid;
END;

-- Recover indices

CREATE INDEX idx_fe_flow_id ON flow_events (flow_id);

/* ------------------------------ */

-- Unified view of all events

CREATE VIEW flow_system_events AS
    SELECT event_id, 'configurations' as source_stream, event_type, event_time, event_payload
        FROM flow_configuration_events

    UNION ALL

    SELECT event_id, 'triggers' as source_stream, event_type, event_time, event_payload
        FROM flow_trigger_events

    UNION ALL

    SELECT event_id, 'flows' as source_stream, event_type, event_time, event_payload
        FROM flow_events;

/* ------------------------------ */

-- Projector offsets table

CREATE TABLE flow_system_projected_offsets (
    projector TEXT PRIMARY KEY,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    -- Note: no tx_id column needed in SQLite implementation
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

/* ------------------------------ */

PRAGMA foreign_keys = ON;

/* ------------------------------ */
