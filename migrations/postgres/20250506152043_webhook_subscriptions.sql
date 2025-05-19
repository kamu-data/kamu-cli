/* ------------------------------ */

CREATE TYPE webhook_subscription_status AS ENUM (
  'UNVERIFIED',
  'ENABLED',
  'PAUSED',
  'UNREACHABLE',
  'REMOVED'
);


/* ------------------------------ */

CREATE TABLE webhook_subscriptions (
  id UUID PRIMARY KEY,
  dataset_id VARCHAR(100),
  event_types VARCHAR(64)[] NOT NULL,
  status webhook_subscription_status NOT NULL DEFAULT 'UNVERIFIED',
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

CREATE SEQUENCE webhook_subscription_event_seq AS BIGINT;

/* ------------------------------ */

CREATE TABLE webhook_subscription_events (
  event_id BIGINT PRIMARY KEY DEFAULT nextval('webhook_subscription_event_seq'),
  subscription_id UUID NOT NULL REFERENCES webhook_subscriptions(id),
  created_at TIMESTAMPTZ NOT NULL,
  event_type VARCHAR(50) NOT NULL,
  event_payload JSONB NOT NULL
);

CREATE INDEX idx_webhook_subscription_events_subscription_id
  ON webhook_subscription_events (subscription_id);

/* ------------------------------ */
