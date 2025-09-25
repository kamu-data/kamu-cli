/* ------------------------------ */

CREATE SEQUENCE flow_system_event_id_seq AS BIGINT;

/* ------------------------------ */

CREATE TYPE flow_system_stream_type AS ENUM ('flows', 'triggers', 'configurations');

/* ------------------------------ */

CREATE TABLE flow_system_events
(
    event_id      BIGINT PRIMARY KEY DEFAULT NEXTVAL('flow_system_event_id_seq'),
    source_stream flow_system_stream_type NOT NULL,
    source_event_id BIGINT NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL,
    inserted_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    -- Idempotency (prevents double-insert from triggers/backfills)
    CONSTRAINT ux_flow_system_events UNIQUE (source_stream, source_event_id)
);

CREATE INDEX ON flow_system_events (source_stream, source_event_id);

ALTER SEQUENCE flow_system_event_id_seq OWNED BY flow_system_events.event_id;

/* ------------------------------ */

CREATE OR REPLACE FUNCTION merge_flow_event()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO flow_system_events(source_stream, source_event_id, occurred_at)
        VALUES ('flows'::flow_system_stream_type, NEW.event_id, NEW.event_time)
        ON CONFLICT (source_stream, source_event_id) DO NOTHING;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION merge_flow_trigger_event()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO flow_system_events(source_stream, source_event_id, occurred_at)
        VALUES ('triggers'::flow_system_stream_type, NEW.event_id, NEW.event_time)
        ON CONFLICT (source_stream, source_event_id) DO NOTHING;
    RETURN NULL;
END $$;

CREATE OR REPLACE FUNCTION merge_flow_configuration_event()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO flow_system_events(source_stream, source_event_id, occurred_at)
        VALUES ('configurations'::flow_system_stream_type, NEW.event_id, NEW.event_time)
        ON CONFLICT (source_stream, source_event_id) DO NOTHING;
    RETURN NULL;
END $$;

CREATE TRIGGER flow_event_into_merged
    AFTER INSERT ON flow_events
    FOR EACH ROW EXECUTE FUNCTION merge_flow_event();

CREATE TRIGGER flow_trigger_event_into_merged
    AFTER INSERT ON flow_trigger_events
    FOR EACH ROW EXECUTE FUNCTION merge_flow_trigger_event();

CREATE TRIGGER flow_configuration_event_into_merged
    AFTER INSERT ON flow_configuration_events
    FOR EACH ROW EXECUTE FUNCTION merge_flow_configuration_event();

/* ------------------------------ */

-- Backfill existing events into the merged table

WITH src AS (
    SELECT
        'flows'::flow_system_stream_type       AS source_stream,
        f.event_id                             AS source_event_id,
        f.event_time                           AS occurred_at
    FROM flow_events f

    UNION ALL

    SELECT
        'triggers'::flow_system_stream_type,
        t.event_id,
        t.event_time
    FROM flow_trigger_events t

    UNION ALL

    SELECT
        'configurations'::flow_system_stream_type,
        c.event_id,
        c.event_time
    FROM flow_configuration_events c
)
INSERT INTO flow_system_events (source_stream, source_event_id, occurred_at, inserted_at)
    SELECT source_stream, source_event_id, occurred_at, NOW()
    FROM src
    ORDER BY occurred_at, source_stream, source_event_id;  -- stable, human-friendly order

/* ------------------------------ */

CREATE OR REPLACE FUNCTION notify_flow_system_merged_events()
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

CREATE TRIGGER flow_system_merged_events_notify
    AFTER INSERT ON flow_system_events
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION notify_flow_system_merged_events();

/* ------------------------------ */

CREATE TABLE flow_system_projected_events (
    projector TEXT NOT NULL PRIMARY KEY,
    done  int8multirange NOT NULL DEFAULT '{}'::int8multirange,
    updated_at timestamptz NOT NULL DEFAULT now()
);

/* ------------------------------ */
