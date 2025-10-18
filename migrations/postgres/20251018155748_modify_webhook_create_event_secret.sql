/* ------------------------------ */

UPDATE webhook_subscription_events
SET event_payload = jsonb_set(
    event_payload,
    '{Created,secret}',
    jsonb_build_object(
        'value',
        (
            SELECT jsonb_agg(get_byte(decoded, gs))
            FROM (
                SELECT
                    decode(event_payload->'Created'->>'secret', 'hex') AS decoded
            ) AS d,
            generate_series(0, length(d.decoded) - 1) AS gs
        ),
        'nonce', 'null'::jsonb
    )
)
WHERE event_type = 'WebhookSubscriptionEventCreated'
  AND jsonb_typeof(event_payload->'Created'->'secret') = 'string';

/* ------------------------------ */
