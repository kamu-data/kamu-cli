/* ------------------------------ */

CREATE TABLE account_quota_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    account_id    TEXT NOT NULL,
    quota_type    TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    event_payload JSONB NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

CREATE INDEX idx_account_quota_events_account_type
    ON account_quota_events (account_id, quota_type);

CREATE INDEX idx_account_quota_events_event_time
    ON account_quota_events (event_time);

/* ------------------------------ */
