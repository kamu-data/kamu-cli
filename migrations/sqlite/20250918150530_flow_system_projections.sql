/* ------------------------------ */

PRAGMA foreign_keys = OFF;

/* ------------------------------ */

UPDATE flows SET last_event_id = NULL;
DELETE FROM flows;
DELETE FROM flow_ids;

DROP TABLE flow_events;
DROP TABLE flow_trigger_events;
DROP TABLE flow_configuration_events;

UPDATE sqlite_sequence SET seq = 0 WHERE name = 'flow_ids';

DELETE FROM sqlite_sequence WHERE name = 'flow_events';
DELETE FROM sqlite_sequence WHERE name = 'flow_configuration_events';
DELETE FROM sqlite_sequence WHERE name = 'flow_trigger_events';

/* ------------------------------ */

CREATE TABLE IF NOT EXISTS flow_event_global_counter (
    name TEXT PRIMARY KEY CHECK (name = 'global'),
    val  INTEGER NOT NULL
);

INSERT OR IGNORE INTO flow_event_global_counter(name, val) VALUES ('global', 0);

/* ------------------------------ */

CREATE TABLE flow_trigger_events (
    event_id INTEGER PRIMARY KEY NOT NULL,
    flow_type VARCHAR(100) NOT NULL,
    scope_data JSONB NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE TRIGGER flow_trigger_events_assign_gid
    AFTER INSERT ON flow_trigger_events
BEGIN
    UPDATE flow_event_global_counter SET val = val + 1 WHERE name = 'global';
    UPDATE flow_trigger_events
        SET event_id = (SELECT val FROM flow_event_global_counter WHERE name = 'global')
        WHERE rowid = NEW.rowid;
END;

CREATE INDEX idx_flow_trigger_events_dataset_scope
    ON flow_trigger_events (flow_type, json_extract(scope_data, '$.dataset_id'))
    WHERE json_extract(scope_data, '$.type') = 'Dataset';

CREATE INDEX idx_flow_trigger_events_system_scope
    ON flow_trigger_events (flow_type)
    WHERE json_extract(scope_data, '$.type') = 'System';    

/* ------------------------------ */

CREATE TABLE flow_configuration_events (
    event_id INTEGER PRIMARY KEY NOT NULL,
    flow_type VARCHAR(100) NOT NULL,
    scope_data JSONB NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE TRIGGER flow_configuration_events_assign_event_id
    AFTER INSERT ON flow_configuration_events
BEGIN
    UPDATE flow_event_global_counter SET val = val + 1 WHERE name = 'global';
    UPDATE flow_configuration_events
        SET event_id = (SELECT val FROM flow_event_global_counter WHERE name = 'global')
        WHERE rowid = NEW.rowid;
END;

CREATE INDEX idx_flow_configuration_events_dataset_scope
    ON flow_configuration_events (flow_type, json_extract(scope_data, '$.dataset_id'))
    WHERE json_extract(scope_data, '$.type') = 'Dataset';

CREATE INDEX idx_flow_configuration_events_system_scope
    ON flow_configuration_events (flow_type)
    WHERE json_extract(scope_data, '$.type') = 'System';

/* ------------------------------ */

CREATE TABLE flow_events
(
    event_id      INTEGER PRIMARY KEY NOT NULL,
    flow_id       BIGINT NOT NULL REFERENCES flows(flow_id),
    event_type    VARCHAR(50) NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE TRIGGER flow_events_assign_event_id
    AFTER INSERT ON flow_events
BEGIN
    UPDATE flow_event_global_counter SET val = val + 1 WHERE name = 'global';
    UPDATE flow_events
        SET event_id = (SELECT val FROM flow_event_global_counter WHERE name = 'global')
        WHERE rowid = NEW.rowid;
END;

CREATE INDEX idx_flow_events_flow_id ON flow_events (flow_id);

/* ------------------------------ */

CREATE TABLE flow_system_projected_offsets (
  projector TEXT PRIMARY KEY,
  last_event_id BIGINT NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

/* ------------------------------ */

PRAGMA foreign_keys = ON;

/* ------------------------------ */
