/* ------------------------------ */

UPDATE flows SET last_event_id = NULL;
DELETE FROM flow_events;
DELETE FROM flows;
DELETE FROM flow_ids;

DELETE FROM flow_configuration_events;
DELETE FROM flow_trigger_events;

UPDATE sqlite_sequence SET seq = 0 WHERE name = 'flow_events';
UPDATE sqlite_sequence SET seq = 0 WHERE name = 'flow_ids';
UPDATE sqlite_sequence SET seq = 0 WHERE name = 'flow_configuration_events';
UPDATE sqlite_sequence SET seq = 0 WHERE name = 'flow_trigger_events';

/* ------------------------------ */

UPDATE tasks SET last_event_id = NULL;
DELETE FROM task_events;
DELETE FROM tasks;
DELETE FROM task_ids;

UPDATE sqlite_sequence SET seq = 0 WHERE name = 'task_events';
UPDATE sqlite_sequence SET seq = 0 WHERE name = 'task_ids';

/* ------------------------------ */

DELETE FROM outbox_messages;
DELETE from outbox_message_consumptions;

UPDATE sqlite_sequence SET seq = 0 WHERE name = 'outbox_messages';

/* ------------------------------ */
