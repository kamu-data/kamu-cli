/* ------------------------------ */

-- Dataset update subscriptions continue from the head of their newest
-- successful delivery, or else from the current head of the dataset
INSERT INTO webhook_watermarks (subscription_id, event_type, watermark)
SELECT s.id, 'DATASET.REF.UPDATED', jsonb_build_object('head', h.head)
FROM webhook_subscriptions s
CROSS JOIN LATERAL (
    SELECT COALESCE(
        (
            SELECT COALESCE(d.request_payload ->> 'new_hash', d.request_payload ->> 'newHash')
            FROM webhook_deliveries d
            WHERE d.subscription_id = s.id
                AND d.event_type = 'DATASET.REF.UPDATED'
                AND d.response_code BETWEEN 200 AND 299
            ORDER BY d.requested_at DESC
            LIMIT 1
        ),
        (
            SELECT 'f1620' || encode(r.block_hash_bin, 'hex')
            FROM dataset_references r
            WHERE r.dataset_id = s.dataset_id AND r.block_ref_name = 'head'
        )
    ) AS head
) h
WHERE s.status <> 'REMOVED'
    AND 'DATASET.REF.UPDATED' = ANY(s.event_types)
    AND h.head IS NOT NULL;

/* ------------------------------ */
