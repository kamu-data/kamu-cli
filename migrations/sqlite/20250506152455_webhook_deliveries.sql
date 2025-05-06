/* ------------------------------ */

CREATE TABLE webhook_deliveries (
    task_id BIGINT NOT NULL REFERENCES tasks(task_id),
    attempt_number INT NOT NULL,
    event_id UUID NOT NULL REFERENCES webhook_events(id) ON DELETE CASCADE,
    subscription_id UUID NOT NULL REFERENCES webhook_subscriptions(id),
    request_headers JSONB NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    response_code INT,
    response_body TEXT,
    response_headers JSONB,
    response_at TIMESTAMPTZ,

    PRIMARY KEY (task_id, attempt_number)
);

CREATE INDEX idx_webhook_deliveries_event_id
  ON webhook_deliveries (event_id);

CREATE INDEX idx_webhook_deliveries_subscription_id
  ON webhook_deliveries (subscription_id);


/* ------------------------------ */
