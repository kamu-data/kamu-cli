/* ------------------------------ */

CREATE TABLE webhook_events (
  id UUID PRIMARY KEY,
  event_type VARCHAR(64) NOT NULL,
  payload JSONB NOT NULL,
  created_at timestamptz NOT NULL
);

CREATE INDEX idx_webhook_events_created_at_desc
  ON webhook_events (created_at DESC);

/* ------------------------------ */
