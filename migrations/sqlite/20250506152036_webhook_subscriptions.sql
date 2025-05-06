/* ------------------------------ */

CREATE TABLE webhook_subscriptions (
    id CHAR(36) PRIMARY KEY NOT NULL,
    target_url VARCHAR(2048) NOT NULL,
    dataset_id VARCHAR(100) NOT NULL,
    events TEXT NOT NULL, -- Store as a comma-separated string
    secret VARCHAR(128) NOT NULL,
    status VARCHAR(15) NOT NULL DEFAULT 'UNVERIFIED' CHECK (
        status IN ('UNVERIFIED', 'ENABLED', 'PAUSED', 'UNREACHABLE', 'REMOVED')
    ),
    label VARCHAR(100),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL    
);

/* ------------------------------ */

CREATE TABLE webhook_subscription_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  subscription_id CHAR(36) NOT NULL REFERENCES webhook_subscriptions(id),
  created_at TIMESTAMPTZ NOT NULL,
  event_type VARCHAR(50) NOT NULL,
  event_payload JSONB NOT NULL
);

CREATE INDEX idx_webhook_subscription_events_subscription_id
  ON webhook_subscription_events (subscription_id);

/* ------------------------------ */
