/* ------------------------------ */

-- Add transaction_id column
ALTER TABLE outbox_messages
    ADD COLUMN tx_id xid8 NOT NULL DEFAULT pg_current_xact_id();

-- We will be scanning in (tx_id, message_id) order, so index accordingly
CREATE INDEX idx_om_tx_order ON outbox_messages (tx_id, message_id);

/* ------------------------------ */

CREATE OR REPLACE FUNCTION notify_outbox_messages()
    RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('outbox_messages_ready', '');
    RETURN NULL;
END $$;

CREATE TRIGGER outbox_notify
    AFTER INSERT ON outbox_messages
    REFERENCING NEW TABLE AS new_rows
    FOR EACH STATEMENT EXECUTE FUNCTION notify_outbox_messages();

/* ------------------------------ */

ALTER TABLE outbox_message_consumptions
    ADD COLUMN last_tx_id xid8 NOT NULL DEFAULT '0'::xid8;

/* ------------------------------ */
