/* ------------------------------ */

-- Run history only: flow configurations and triggers stay, and the flow agent
-- schedules enabled flows from them at startup. AUTOINCREMENT keeps the task
-- and flow IDs from being reused; the global flow event counter also numbers
-- the kept trigger and configuration events, and projector offsets stay valid
-- against it

UPDATE tasks SET last_event_id = NULL;
DELETE FROM task_events;
DELETE FROM tasks;
DELETE FROM task_ids;

-- Task progress messages carry task outcomes
DELETE FROM outbox_messages
WHERE producer_name = 'dev.kamu.domain.task-system.TaskAgent';

UPDATE flows SET last_event_id = NULL;
DELETE FROM flow_activation_links;
DELETE FROM flow_events;
DELETE FROM flows;
DELETE FROM flow_ids;

UPDATE flow_process_states SET running_since = NULL;

/* ------------------------------ */
