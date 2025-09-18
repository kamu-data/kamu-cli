/* ------------------------------ */

CREATE TABLE flow_system_events (
    event_id        INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    source_stream   TEXT NOT NULL CHECK (source_stream IN ('flows','triggers','configurations')),
    source_event_id INTEGER NOT NULL,
    occurred_at     TIMESTAMPTZ NOT NULL,
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT (datetime('now')),
  UNIQUE (source_stream, source_event_id)
);

CREATE INDEX idx_flow_system_events_stream_srcid
    ON flow_system_events (source_stream, source_event_id);

/* ------------------------------ */

CREATE TRIGGER flow_event_into_merged
    AFTER INSERT ON flow_events
BEGIN
    INSERT OR IGNORE INTO flow_system_events (source_stream, source_event_id, occurred_at, inserted_at)
        VALUES ('flows', NEW.event_id, NEW.event_time, datetime('now'));
END;

CREATE TRIGGER flow_trigger_event_into_merged
    AFTER INSERT ON flow_trigger_events
BEGIN
    INSERT OR IGNORE INTO flow_system_events (source_stream, source_event_id, occurred_at, inserted_at)
        VALUES ('triggers', NEW.event_id, NEW.event_time, datetime('now'));
END;

CREATE TRIGGER flow_configuration_event_into_merged
    AFTER INSERT ON flow_configuration_events
BEGIN
    INSERT OR IGNORE INTO flow_system_events (source_stream, source_event_id, occurred_at, inserted_at)
        VALUES ('configurations', NEW.event_id, NEW.event_time, datetime('now'));
END;

/* ------------------------------ */

WITH src AS (
    SELECT 'flows' AS source_stream, f.event_id AS source_event_id, f.event_time AS occurred_at
    FROM flow_events f
  
    UNION ALL

    SELECT 'triggers', t.event_id AS source_event_id, t.event_time AS occurred_at
    FROM flow_trigger_events t
  
    UNION ALL

    SELECT 'configurations', c.event_id AS source_event_id, c.event_time AS occurred_at
    FROM flow_configuration_events c
)
INSERT INTO flow_system_events (source_stream, source_event_id, occurred_at, inserted_at)
    SELECT source_stream, source_event_id, occurred_at, datetime('now')
    FROM src
    ORDER BY occurred_at, source_stream, source_event_id;

/* ------------------------------ */

CREATE TABLE flow_system_projected_events (
  projector TEXT NOT NULL,
  event_id  INTEGER NOT NULL,
  PRIMARY KEY (projector, event_id)
);

/* ------------------------------ */
