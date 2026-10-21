/*
 * Resources report a failed reconciliation or cleanup through the ODF
 * `ReconciliationError` condition (`code`, `message`), present only while the
 * failure stands. The kamu `Ready` and `Reconciling` conditions are dropped:
 * the phase already tells what they did.
 *
 * Stored statuses, in `StatusUpdated` events and in `resources.status`, are
 * rewritten in place:
 *   - `Ready` and `Reconciling` are removed
 *   - `Ready=False` becomes `ReconciliationError` with its reason as the code,
 *     in kebab-case, and its message
 */

CREATE FUNCTION pg_temp.reconciliation_error_code(reason TEXT) RETURNS TEXT AS $$
    SELECT CASE reason
        WHEN 'MissingVariableRef'      THEN 'missing-variable-ref'
        WHEN 'MissingSecretRef'        THEN 'missing-secret-ref'
        WHEN 'InvalidConfiguration'    THEN 'invalid-configuration'
        WHEN 'DependencyUnavailable'   THEN 'dependency-unavailable'
        WHEN 'InternalError'           THEN 'internal-error'
        WHEN 'internal_error'          THEN 'internal-error'
        WHEN 'concurrent_modification' THEN 'concurrent-modification'
        WHEN 'CleanupFailed'           THEN 'cleanup-failed'
        ELSE reason
    END
$$ LANGUAGE SQL IMMUTABLE;

CREATE FUNCTION pg_temp.odf_resource_conditions(conditions JSONB) RETURNS JSONB AS $$
    SELECT
        (conditions
            - 'https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready'
            - 'https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling')
        || CASE
            WHEN ready ->> 'value' = 'False' THEN jsonb_build_object(
                'https://opendatafabric.org/schemas/resources/v1alpha1/ReconciliationError',
                jsonb_build_object(
                    'code',    pg_temp.reconciliation_error_code(ready ->> 'reason'),
                    'message', COALESCE(ready ->> 'message', '')
                )
            )
            ELSE '{}'::JSONB
        END
    FROM (
        SELECT conditions -> 'https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready' AS ready
    ) r
$$ LANGUAGE SQL IMMUTABLE;

/* ------------------------------ */

UPDATE resource_events
SET event_payload = jsonb_set(
    event_payload,
    '{StatusUpdated,new_status,conditions}',
    pg_temp.odf_resource_conditions(event_payload #> '{StatusUpdated,new_status,conditions}')
)
WHERE event_type = 'StatusUpdated'
    AND event_payload #> '{StatusUpdated,new_status}' ? 'conditions';

UPDATE resources
SET status = jsonb_set(
    status,
    '{conditions}',
    pg_temp.odf_resource_conditions(status -> 'conditions')
)
WHERE status ? 'conditions';
