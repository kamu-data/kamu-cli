/* ------------------------------ */

-- Reset all flow-related tables to start fresh

UPDATE flows SET last_event_id = NULL;
DELETE FROM flow_events;
DELETE FROM flows;

DELETE FROM flow_configuration_events;
DELETE FROM flow_trigger_events;

ALTER SEQUENCE flow_id_seq RESTART WITH 1;

/* ------------------------------ */

-- Bind all event tables to a single sequence

CREATE SEQUENCE flow_system_event_id_seq AS BIGINT;

ALTER TABLE flow_configuration_events
    ALTER COLUMN event_id SET DEFAULT nextval('flow_system_event_id_seq'::regclass);

ALTER TABLE flow_trigger_events
    ALTER COLUMN event_id SET DEFAULT nextval('flow_system_event_id_seq'::regclass);

ALTER TABLE flow_events
    ALTER COLUMN event_id SET DEFAULT nextval('flow_system_event_id_seq'::regclass);

DROP SEQUENCE flow_event_id_seq;
DROP SEQUENCE flow_configuration_event_id_seq;
DROP SEQUENCE flow_trigger_event_id_seq;

/* ------------------------------ */

-- Add transaction_id column to events

ALTER TABLE flow_configuration_events
    ADD COLUMN tx_id xid8 NOT NULL DEFAULT pg_current_xact_id();

ALTER TABLE flow_trigger_events
    ADD COLUMN tx_id xid8 NOT NULL DEFAULT pg_current_xact_id();

ALTER TABLE flow_events
    ADD COLUMN tx_id xid8 NOT NULL DEFAULT pg_current_xact_id();

-- Note: we will be scanning in (tx_id, event_id) order, so index accordingly
CREATE INDEX idx_flow_configuration_events_tx_order ON flow_configuration_events (tx_id, event_id);
CREATE INDEX idx_flow_trigger_events_tx_order ON flow_trigger_events (tx_id, event_id);
CREATE INDEX idx_flow_events_tx_order ON flow_events (tx_id, event_id);

/* ------------------------------ */

-- Each push to the event tables will notify shared listener

CREATE OR REPLACE FUNCTION notify_flow_system_events()
    RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
    min_id BIGINT; max_id BIGINT;
BEGIN
    SELECT min(event_id), max(event_id) INTO min_id, max_id FROM new_rows;
    PERFORM pg_notify(
        'flow_system_events_ready',
        json_build_object('min', min_id, 'max', max_id)::text
    );
    RETURN NULL;
END $$;

CREATE TRIGGER flow_events_notify
    AFTER INSERT ON flow_events
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION notify_flow_system_events();

CREATE TRIGGER flow_trigger_events_notify
    AFTER INSERT ON flow_trigger_events
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION notify_flow_system_events();

CREATE TRIGGER flow_configuration_events_notify
    AFTER INSERT ON flow_configuration_events
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION notify_flow_system_events();

/* ------------------------------ */

-- Unified view of all events

CREATE OR REPLACE VIEW flow_system_events AS
    SELECT event_id, tx_id, 'configurations' as source_stream, event_type, event_time, event_payload
        FROM flow_configuration_events

    UNION ALL

    SELECT event_id, tx_id, 'triggers' as source_stream, event_type, event_time, event_payload
    FROM flow_trigger_events

    UNION ALL

    SELECT event_id, tx_id, 'flows' as source_stream, event_type, event_time, event_payload
    FROM flow_events;

/* ------------------------------ */

-- Projector offsets table (with transaction tracking)

CREATE TABLE flow_system_projected_offsets (
    projector TEXT NOT NULL PRIMARY KEY,
    last_tx_id xid8 NOT NULL DEFAULT '0'::xid8,
    last_event_id BIGINT NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now()
);

/* ------------------------------ */
