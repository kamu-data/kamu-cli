/*
 * Resources report a failed reconciliation or cleanup through the ODF
 * `ReconciliationError` condition (`code`, `message`), present only while the
 * failure stands. The kamu `Ready` and `Reconciling` conditions are dropped:
 * the phase already tells what they did.
 *
 * Stored statuses, in `StatusUpdated` events and in `resources.status`, are
 * rewritten in place:
 *   - `Ready=False` becomes `ReconciliationError` with its reason as the code,
 *     in kebab-case, and its message
 *   - `Ready` and `Reconciling` are removed
 */

CREATE TEMP TABLE reconciliation_error_codes (
    reason TEXT PRIMARY KEY,
    code   TEXT NOT NULL
);

INSERT INTO reconciliation_error_codes (reason, code) VALUES
    ('MissingVariableRef',      'missing-variable-ref'),
    ('MissingSecretRef',        'missing-secret-ref'),
    ('InvalidConfiguration',    'invalid-configuration'),
    ('DependencyUnavailable',   'dependency-unavailable'),
    ('InternalError',           'internal-error'),
    ('internal_error',          'internal-error'),
    ('concurrent_modification', 'concurrent-modification'),
    ('CleanupFailed',           'cleanup-failed');

/* ------------------------------ */

UPDATE resource_events
SET event_payload = json_set(
    event_payload,
    '$.StatusUpdated.new_status.conditions."https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError"',
    json_object(
        'code', COALESCE(
            (
                SELECT c.code
                FROM reconciliation_error_codes c
                WHERE c.reason = json_extract(event_payload, '$.StatusUpdated.new_status.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".reason')
            ),
            json_extract(event_payload, '$.StatusUpdated.new_status.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".reason')
        ),
        'message', COALESCE(
            json_extract(event_payload, '$.StatusUpdated.new_status.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".message'),
            ''
        )
    )
)
WHERE event_type = 'StatusUpdated'
    AND json_extract(event_payload, '$.StatusUpdated.new_status.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".value') = 'False';

UPDATE resource_events
SET event_payload = json_remove(
    event_payload,
    '$.StatusUpdated.new_status.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready"',
    '$.StatusUpdated.new_status.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling"'
)
WHERE event_type = 'StatusUpdated';

/* ------------------------------ */

UPDATE resources
SET status = json_set(
    status,
    '$.conditions."https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError"',
    json_object(
        'code', COALESCE(
            (
                SELECT c.code
                FROM reconciliation_error_codes c
                WHERE c.reason = json_extract(status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".reason')
            ),
            json_extract(status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".reason')
        ),
        'message', COALESCE(
            json_extract(status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".message'),
            ''
        )
    )
)
WHERE json_extract(status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".value') = 'False';

UPDATE resources
SET status = json_remove(
    status,
    '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready"',
    '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling"'
)
WHERE status IS NOT NULL;

DROP TABLE reconciliation_error_codes;
