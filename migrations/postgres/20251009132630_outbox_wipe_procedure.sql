/* ------------------------------ */

CREATE OR REPLACE PROCEDURE wipe_outbox_data()
LANGUAGE plpgsql
AS $$
BEGIN
    -- Truncate tables
    TRUNCATE TABLE outbox_message_consumptions;
    TRUNCATE TABLE outbox_messages;

    -- Explicitly reset the sequence
    PERFORM setval('outbox_message_id_seq', 1, false);
END;
$$;

/* ------------------------------ */

CALL wipe_outbox_data();

/* ------------------------------ */
