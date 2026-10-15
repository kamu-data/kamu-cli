/* ------------------------------ */

CREATE TABLE webhook_watermarks (
    subscription_id UUID NOT NULL REFERENCES webhook_subscriptions(id),
    event_type VARCHAR(64) NOT NULL,
    watermark JSONB NOT NULL,

    PRIMARY KEY (subscription_id, event_type)
);

/* ------------------------------ */
