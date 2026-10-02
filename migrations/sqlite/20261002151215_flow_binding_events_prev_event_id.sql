/* ------------------------------ */

-- Each saved batch records the binding event it was based on in its first event,
-- 0 when the binding had none; two writers based on the same event collide on the
-- unique index, which detects concurrent modifications. Existing events form one
-- batch each.

ALTER TABLE flow_trigger_events ADD COLUMN prev_event_id BIGINT;

UPDATE flow_trigger_events
    SET prev_event_id = chain.prev_event_id
    FROM (
        SELECT
            event_id,
            COALESCE(
                LAG(event_id) OVER (PARTITION BY flow_type, scope_data ORDER BY event_id),
                0
            ) AS prev_event_id
        FROM flow_trigger_events
    ) AS chain
    WHERE flow_trigger_events.event_id = chain.event_id;

CREATE UNIQUE INDEX idx_flow_trigger_events_binding_prev_event
    ON flow_trigger_events (flow_type, scope_data, prev_event_id)
    WHERE prev_event_id IS NOT NULL;

/* ------------------------------ */

ALTER TABLE flow_configuration_events ADD COLUMN prev_event_id BIGINT;

UPDATE flow_configuration_events
    SET prev_event_id = chain.prev_event_id
    FROM (
        SELECT
            event_id,
            COALESCE(
                LAG(event_id) OVER (PARTITION BY flow_type, scope_data ORDER BY event_id),
                0
            ) AS prev_event_id
        FROM flow_configuration_events
    ) AS chain
    WHERE flow_configuration_events.event_id = chain.event_id;

CREATE UNIQUE INDEX idx_flow_configuration_events_binding_prev_event
    ON flow_configuration_events (flow_type, scope_data, prev_event_id)
    WHERE prev_event_id IS NOT NULL;

/* ------------------------------ */
