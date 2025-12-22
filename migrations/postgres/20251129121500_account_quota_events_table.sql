/* ------------------------------ */

CREATE TABLE account_quota_events (
    id           BIGSERIAL PRIMARY KEY,
    account_id   TEXT NOT NULL,
    quota_type   TEXT NOT NULL,
    event_type   TEXT NOT NULL,
    event_payload JSONB NOT NULL,
    event_time   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_account_quota_events_account_type
    ON account_quota_events (account_id, quota_type);

CREATE INDEX idx_account_quota_events_event_time
    ON account_quota_events (event_time);

/* ------------------------------ */