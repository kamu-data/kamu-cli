/* ------------------------------ */

UPDATE flows SET last_event_id = NULL;
DELETE FROM flow_events;
DELETE FROM flows;

DELETE FROM flow_configuration_events;
DELETE FROM flow_trigger_events;

ALTER SEQUENCE flow_id_seq RESTART WITH 1;

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

CREATE TABLE flow_system_projected_events (
    projector TEXT NOT NULL PRIMARY KEY,
    done  int8multirange NOT NULL DEFAULT '{}'::int8multirange,
    updated_at timestamptz NOT NULL DEFAULT now()
);

/* ------------------------------ */
