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
  target_url VARCHAR(2048) NOT NULL,
  dataset_id VARCHAR(100),
  events VARCHAR(64)[] NOT NULL,
  secret VARCHAR(128) NOT NULL,
  status webhook_subscription_status NOT NULL DEFAULT 'UNVERIFIED',
  label VARCHAR(100),
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
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

CREATE TYPE webhook_subscription_event_type AS ENUM (
  'SUBSCRIPTION_CREATED',
  'SUBSCRIPTION_UPDATED',
  'SUBSCRIPTION_SECRET_ROTATED',
  'SUBSCRIPTION_ENABLED',
  'SUBSCRIPTION_PAUSED',
  'SUBSCRIPTION_RESUMED',
  'SUBSCRIPTION_MARKED_UNREACHABLE',
  'SUBSCRIPTION_REACTIVATED',
  'SUBSCRIPTION_REMOVED'
);

/* ------------------------------ */

CREATE TABLE webhook_subscription_events (
  id BIGINT PRIMARY KEY DEFAULT nextval('webhook_subscription_event_seq'),
  subscription_id UUID NOT NULL REFERENCES webhook_subscriptions(id),
  created_at TIMESTAMPTZ NOT NULL,
  event_type webhook_subscription_event_type NOT NULL,
  event_payload JSONB NOT NULL
);

CREATE INDEX idx_webhook_subscription_events_subscription_id
  ON webhook_subscription_events (subscription_id);

/* ------------------------------ */
