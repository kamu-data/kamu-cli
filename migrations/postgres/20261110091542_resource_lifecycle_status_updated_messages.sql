/*
 * Version 4 of the resource lifecycle messages reports every status change as
 * `StatusUpdated`, which replaces `ReconciliationSucceeded` and
 * `ReconciliationFailed`. Stored version 3 messages are rewritten, so pending
 * ones stay deliverable:
 *   - a reconciliation outcome becomes `StatusUpdated` with the resource's id,
 *     schema and headers, and its status without the conditions, taken from
 *     the snapshot the outcome carried. A failure also reports the
 *     `ReconciliationError` it set, a success no changed condition
 *   - every other message of the producer keeps its content
 */

/* ------------------------------ */

UPDATE outbox_messages AS m
SET content_json = jsonb_build_object(
    'StatusUpdated', jsonb_build_object(
        'event_time', outcome.message -> 'event_time',
        'resource', jsonb_build_object(
            'id', outcome.message -> 'resource' -> 'id',
            'schema', outcome.message -> 'resource' -> 'schema',
            'headers', outcome.message -> 'resource' -> 'headers'
        ),
        'status', (outcome.message -> 'resource' -> 'status') - 'conditions',
        'changed_conditions', CASE
            WHEN outcome.failed
                AND outcome.message -> 'resource' -> 'status' -> 'conditions'
                    ? 'https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError'
            THEN jsonb_build_object(
                'https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError',
                jsonb_build_object('Set', jsonb_build_object(
                    'value',
                    outcome.message -> 'resource' -> 'status' -> 'conditions'
                        -> 'https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError'
                ))
            )
            ELSE '{}'::jsonb
        END
    )
)
FROM (
    SELECT
        message_id,
        COALESCE(
            content_json -> 'ReconciliationSucceeded',
            content_json -> 'ReconciliationFailed'
        ) AS message,
        content_json ? 'ReconciliationFailed' AS failed
    FROM outbox_messages
    WHERE producer_name = 'dev.kamu.domain.resources.ResourceService'
        AND version = 3
        AND (content_json ? 'ReconciliationSucceeded' OR content_json ? 'ReconciliationFailed')
) AS outcome
WHERE m.message_id = outcome.message_id;

/* ------------------------------ */

UPDATE outbox_messages
SET version = 4
WHERE producer_name = 'dev.kamu.domain.resources.ResourceService'
    AND version = 3;

/* ------------------------------ */
