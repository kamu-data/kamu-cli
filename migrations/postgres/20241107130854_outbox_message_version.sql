ALTER TABLE outbox_messages ADD version INT;

/* -----------------------------------------------
Initial one time migration to set message version
-------------------------------------------------- */

UPDATE outbox_messages set version = 1;

/* ----------------------------------------------- */
