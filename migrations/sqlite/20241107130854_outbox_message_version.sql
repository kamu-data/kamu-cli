ALTER TABLE outbox_messages ADD 'version' INT;

CREATE INDEX idx_outbox_messages_version ON outbox_messages('version');

/* -----------------------------------------------
Initial one time migration to set message version
-------------------------------------------------- */

UPDATE outbox_messages set 'version' = 1;

/* ----------------------------------------------- */
