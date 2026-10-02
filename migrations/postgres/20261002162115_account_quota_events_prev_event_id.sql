/* ------------------------------ */

-- Each saved batch records the quota event it was based on in its first event,
-- 0 when the quota had none; two writers based on the same event collide on the
-- unique index, which detects concurrent modifications. Existing events form one
-- batch each.

ALTER TABLE account_quota_events ADD COLUMN prev_event_id BIGINT;

UPDATE account_quota_events
    SET prev_event_id = chain.prev_event_id
    FROM (
        SELECT
            id,
            COALESCE(
                LAG(id) OVER (PARTITION BY account_id, quota_type ORDER BY id),
                0
            ) AS prev_event_id
        FROM account_quota_events
    ) AS chain
    WHERE account_quota_events.id = chain.id;

CREATE UNIQUE INDEX idx_account_quota_events_account_type_prev_event
    ON account_quota_events (account_id, quota_type, prev_event_id)
    WHERE prev_event_id IS NOT NULL;

/* ------------------------------ */
