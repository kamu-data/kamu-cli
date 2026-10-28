/* ------------------------------ */

-- Run history only: flow configurations and triggers stay, and the flow agent
-- schedules enabled flows from them at startup. ID sequences are kept so no ID
-- is reused; flow_system_event_id_seq also numbers the kept trigger and
-- configuration events, and projector offsets stay valid against it

CREATE OR REPLACE PROCEDURE wipe_task_data()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE task_events, tasks;

    -- Task progress messages carry task outcomes
    DELETE FROM outbox_messages
    WHERE producer_name = 'dev.kamu.domain.task-system.TaskAgent';
END;
$$;

CREATE OR REPLACE PROCEDURE wipe_flow_run_data()
LANGUAGE plpgsql
AS $$
BEGIN
    TRUNCATE TABLE flow_activation_links, flow_events, flows;

    UPDATE flow_process_states SET running_since = NULL;
END;
$$;

/* ------------------------------ */

CALL wipe_task_data();
CALL wipe_flow_run_data();

/* ------------------------------ */
