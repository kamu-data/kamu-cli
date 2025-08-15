/* ------------------------------ */

UPDATE flows SET last_event_id = NULL;
DELETE FROM flow_events;
DELETE FROM flows;

DELETE FROM flow_configuration_events;
DELETE FROM flow_trigger_events;

ALTER SEQUENCE flow_id_seq RESTART WITH 1;
ALTER SEQUENCE flow_event_id_seq RESTART WITH 1;
ALTER SEQUENCE flow_configuration_event_id_seq RESTART WITH 1;
ALTER SEQUENCE flow_trigger_event_id_seq RESTART WITH 1;

/* ------------------------------ */

UPDATE tasks SET last_event_id = NULL;
DELETE FROM task_events;
DELETE FROM tasks;

ALTER SEQUENCE task_event_id_seq RESTART WITH 1;
ALTER SEQUENCE task_id_seq RESTART WITH 1;

/* ------------------------------ */

DELETE FROM outbox_messages;
DELETE from outbox_message_consumptions;

ALTER SEQUENCE outbox_message_id_seq RESTART WITH 1;

/* ------------------------------ */
