/* ------------------------------ */


CREATE TABLE webhook_deliveries_new (
    delivery_id UUID NOT NULL PRIMARY KEY,
    subscription_id UUID NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    request_payload JSONB NOT NULL,
    request_headers JSONB NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL,
    response_code INT2,
    response_body TEXT,
    response_headers JSONB,
    response_at TIMESTAMPTZ,

    CONSTRAINT webhook_deliveries_subscription_id_fkey 
        FOREIGN KEY (subscription_id) REFERENCES webhook_subscriptions(id)
);

INSERT INTO webhook_deliveries_new (
    delivery_id,
    subscription_id,
    event_type,
    request_payload,
    request_headers,
    requested_at,
    response_code,
    response_body,
    response_headers,
    response_at
)
SELECT
    d.event_id AS delivery_id,
    d.subscription_id,
    e.event_type,
    e.payload,
    d.request_headers,
    d.requested_at,
    d.response_code,
    d.response_body,
    d.response_headers,
    d.response_at
FROM webhook_deliveries d
JOIN webhook_events e ON d.event_id = e.id;

DROP TABLE webhook_deliveries;
DROP TABLE webhook_events;

ALTER TABLE webhook_deliveries_new RENAME TO webhook_deliveries;

CREATE INDEX idx_webhook_deliveries_subscription_id
  ON webhook_deliveries (subscription_id);

/* ------------------------------ */
