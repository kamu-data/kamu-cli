/* ------------------------------ */

DELETE FROM outbox_messages;
DELETE from outbox_message_consumptions;

UPDATE sqlite_sequence SET seq = 0 WHERE name = 'outbox_messages';

/* ------------------------------ */
