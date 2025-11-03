/* ------------------------------ */

UPDATE webhook_subscription_events
SET event_payload = json_set(
  event_payload,
  '$.Created.secret',
  json_object(
    'value',
    (
      WITH RECURSIVE
        -- Original hex string, normalized to lowercase
        s(x) AS (
          SELECT lower(json_extract(event_payload, '$.Created.secret'))
        ),
        -- Index of byte (0 .. byte_count-1)
        idx(i) AS (
          SELECT 0
          UNION ALL
          SELECT i + 1
          FROM idx, s
          WHERE (i + 1) < (length(s.x) / 2)
        ),
        -- Two-char hex pairs for each byte
        pairs(p) AS (
          SELECT substr(s.x, i * 2 + 1, 2)
          FROM idx, s
        ),
        -- Convert each hex pair to integer byte:
        -- value = high_nibble * 16 + low_nibble
        vals(v) AS (
          SELECT
            (instr('0123456789abcdef', substr(p, 1, 1)) - 1) * 16 +
            (instr('0123456789abcdef', substr(p, 2, 1)) - 1)
          FROM pairs
        )
      SELECT json_group_array(v)
      FROM vals
    ),
    'nonce', json('null')
  )
)
WHERE event_type = 'WebhookSubscriptionEventCreated'
  AND json_type(json_extract(event_payload, '$.Created.secret')) = 'text';

/* ------------------------------ */
