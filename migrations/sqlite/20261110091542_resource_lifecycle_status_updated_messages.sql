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

UPDATE outbox_messages
SET content_json = (
    SELECT json_object(
        'StatusUpdated', json_object(
            'event_time', json_extract(outcome.message, '$.event_time'),
            'resource', json_object(
                'id', json_extract(outcome.message, '$.resource.id'),
                'schema', json_extract(outcome.message, '$.resource.schema'),
                'headers', json_extract(outcome.message, '$.resource.headers')
            ),
            'status', json_remove(json_extract(outcome.message, '$.resource.status'), '$.conditions'),
            'changed_conditions', CASE
                WHEN outcome.failed
                    AND json_type(
                        outcome.message,
                        '$.resource.status.conditions."https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError"'
                    ) IS NOT NULL
                THEN json_object(
                    'https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError',
                    json_object('Set', json_object(
                        'value',
                        json_extract(
                            outcome.message,
                            '$.resource.status.conditions."https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError"'
                        )
                    ))
                )
                ELSE json_object()
            END
        )
    )
    FROM (
        SELECT
            COALESCE(
                json_extract(outbox_messages.content_json, '$.ReconciliationSucceeded'),
                json_extract(outbox_messages.content_json, '$.ReconciliationFailed')
            ) AS message,
            json_type(outbox_messages.content_json, '$.ReconciliationFailed') IS NOT NULL AS failed
    ) AS outcome
)
WHERE producer_name = 'dev.kamu.domain.resources.ResourceService'
    AND version = 3
    AND (
        json_type(content_json, '$.ReconciliationSucceeded') IS NOT NULL
        OR json_type(content_json, '$.ReconciliationFailed') IS NOT NULL
    );

/* ------------------------------ */

UPDATE outbox_messages
SET version = 4
WHERE producer_name = 'dev.kamu.domain.resources.ResourceService'
    AND version = 3;

/* ------------------------------ */
