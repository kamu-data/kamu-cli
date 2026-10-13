/* ------------------------------ */

-- Wake the flow agent when a flow gets a new activation time:
-- scheduled for activation, or a retry planned after a failed task

CREATE OR REPLACE FUNCTION notify_flow_activation_scheduled()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('flow_activation_scheduled', '');
    RETURN NULL;
END $$;

-- Row-level to filter by value: every update of a flow rewrites the column,
-- while resets to NULL and unchanged values must not wake the agent.
-- Postgres collapses duplicate notifications per transaction.
-- Inserts need no trigger: a new flow has no activation time yet.
CREATE TRIGGER flows_activation_notify
    AFTER UPDATE OF scheduled_for_activation_at ON flows
    FOR EACH ROW
    WHEN (
        NEW.scheduled_for_activation_at IS NOT NULL
        AND NEW.scheduled_for_activation_at IS DISTINCT FROM OLD.scheduled_for_activation_at
    )
    EXECUTE FUNCTION notify_flow_activation_scheduled();

/* ------------------------------ */
