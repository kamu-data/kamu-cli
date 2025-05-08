/* ------------------------------ */

CREATE TABLE webhook_subscriptions (
    id CHAR(36) PRIMARY KEY NOT NULL,
    dataset_id VARCHAR(100),
    event_types TEXT NOT NULL, -- Store as a comma-separated string
    status VARCHAR(15) NOT NULL DEFAULT 'UNVERIFIED' CHECK (
        status IN ('UNVERIFIED', 'ENABLED', 'PAUSED', 'UNREACHABLE', 'REMOVED')
    ),
    label VARCHAR(100),
    last_event_id BIGINT -- Must be an event id, but don't create FK loop
);

CREATE INDEX idx_webhook_subscription_dataset_status
  ON webhook_subscriptions (dataset_id, status)
  WHERE dataset_id IS NOT NULL;

CREATE UNIQUE INDEX uniq_webhook_subscription_label
  ON webhook_subscriptions (dataset_id, label)  -- NULL dataset_id is fine
  WHERE label IS NOT NULL AND status != 'REMOVED';

/* ------------------------------ */

CREATE TABLE webhook_subscription_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
  subscription_id CHAR(36) NOT NULL REFERENCES webhook_subscriptions(id),
  created_at TIMESTAMPTZ NOT NULL,
  event_type VARCHAR(50) NOT NULL,
  event_payload JSONB NOT NULL
);

CREATE INDEX idx_webhook_subscription_events_subscription_id
  ON webhook_subscription_events (subscription_id);

/* ------------------------------ */
