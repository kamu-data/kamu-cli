/*
 * Resource status is written through one event, `StatusUpdated`, carrying the
 * whole new status, and `resources.status_generation` counts those updates.
 *
 * Stored `ReconciliationStarted` / `ReconciliationSucceeded` /
 * `ReconciliationFailed` events are rewritten in place into `StatusUpdated`
 * events holding the status each of them produced. The statuses are folded
 * per resource in event order, following the rules the code applied to those
 * events:
 *   - `Created` and `SpecUpdated`: phase `Pending`, no conditions
 *   - `ReconciliationStarted`: phase `Reconciling`, observed generation and
 *     time, `Reconciling=True`
 *   - `ReconciliationSucceeded`: phase `Ready`, reconciled generation and
 *     time, `Ready=True`, `Reconciling=False`
 *   - `ReconciliationFailed`: phase `Failed` before any success, `Degraded`
 *     after one, `Ready=False` with reason and message, `Reconciling=False`
 *   - a condition keeps its `lastTransitionTime` when its value is unchanged
 *
 * The folded final status must match `resources.status`, otherwise the
 * migration aborts. The snapshot rows are then derived again the way the
 * rewritten events replay: spec updates keep conditions, the phase is
 * `Pending` while the generation is not observed, and every event moves
 * `updated_at`.
 */

ALTER TABLE resources ADD COLUMN status_generation BIGINT NOT NULL DEFAULT 1;

/* ------------------------------ */

CREATE TEMP TABLE resource_events_ordered ON COMMIT DROP AS
SELECT
    event_id,
    resource_id,
    event_type,
    event_payload -> event_type AS event,
    ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY event_id) AS seq
FROM resource_events;

CREATE UNIQUE INDEX ON resource_events_ordered (resource_id, seq);

/*
 * Temp tables are not analyzed automatically. Without statistics the planner
 * may join the recursive fold below to this table with a full scan per step,
 * which is quadratic in the length of the longest event history.
 */
ANALYZE resource_events_ordered;

/* ------------------------------ */

/* The condition `new_condition` replaces the one stored under `condition_key` */
CREATE FUNCTION pg_temp.set_resource_condition(
    conditions JSONB,
    condition_key TEXT,
    new_condition JSONB
) RETURNS JSONB AS $$
    SELECT conditions || jsonb_build_object(
        condition_key,
        CASE
            WHEN conditions -> condition_key ->> 'value' = new_condition ->> 'value'
                THEN new_condition || jsonb_build_object(
                    'lastTransitionTime', conditions -> condition_key -> 'lastTransitionTime'
                )
            ELSE new_condition
        END
    )
$$ LANGUAGE SQL IMMUTABLE;

/* Fields of a status compared by the guard: timestamps are left out, as their text form varies */
CREATE FUNCTION pg_temp.resource_status_semantics(status JSONB) RETURNS JSONB AS $$
    SELECT jsonb_build_object(
        'phase',                status -> 'phase',
        'observedGeneration',   status -> 'observedGeneration',
        'reconciledGeneration', status -> 'reconciledGeneration',
        'conditions',           COALESCE(
                                    (
                                        SELECT jsonb_object_agg(key, value - 'lastTransitionTime')
                                        FROM jsonb_each(status -> 'conditions')
                                    ),
                                    '{}'::jsonb
                                )
    )
$$ LANGUAGE SQL IMMUTABLE;

/* ------------------------------ */

CREATE TEMP TABLE resource_status_fold ON COMMIT DROP AS
WITH RECURSIVE fold AS (
    SELECT
        e.resource_id,
        e.seq,
        e.event_id,
        e.event_type,
        e.event,
        '{"phase": "Pending", "conditions": {}}'::jsonb AS status
    FROM resource_events_ordered e
    WHERE e.seq = 1

    UNION ALL

    SELECT
        e.resource_id,
        e.seq,
        e.event_id,
        e.event_type,
        e.event,
        CASE e.event_type
            WHEN 'SpecUpdated' THEN
                f.status || '{"phase": "Pending", "conditions": {}}'::jsonb
            WHEN 'ReconciliationStarted' THEN
                f.status || jsonb_build_object(
                    'phase',              'Reconciling',
                    'observedGeneration', e.event -> 'generation',
                    'observedAt',         e.event -> 'event_time',
                    'conditions',         pg_temp.set_resource_condition(
                        f.status -> 'conditions',
                        'https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling',
                        jsonb_build_object(
                            'value',              'True',
                            'reason',             'Processing',
                            'lastTransitionTime', e.event -> 'event_time'
                        )
                    )
                )
            WHEN 'ReconciliationSucceeded' THEN
                f.status || jsonb_build_object(
                    'phase',                'Ready',
                    'reconciledGeneration', e.event -> 'generation',
                    'reconciledAt',         e.event -> 'event_time',
                    'conditions',           pg_temp.set_resource_condition(
                        pg_temp.set_resource_condition(
                            f.status -> 'conditions',
                            'https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready',
                            jsonb_build_object(
                                'value',              'True',
                                'reason',             'Reconciled',
                                'lastTransitionTime', e.event -> 'event_time'
                            )
                        ),
                        'https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling',
                        jsonb_build_object(
                            'value',              'False',
                            'reason',             'Idle',
                            'lastTransitionTime', e.event -> 'event_time'
                        )
                    )
                )
            WHEN 'ReconciliationFailed' THEN
                f.status || jsonb_build_object(
                    'phase',      CASE
                                      WHEN f.status ? 'reconciledGeneration' THEN 'Degraded'
                                      ELSE 'Failed'
                                  END,
                    'conditions', pg_temp.set_resource_condition(
                        pg_temp.set_resource_condition(
                            f.status -> 'conditions',
                            'https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready',
                            jsonb_build_object(
                                'value',              'False',
                                'reason',             e.event -> 'reason',
                                'message',            e.event -> 'message',
                                'lastTransitionTime', e.event -> 'event_time'
                            )
                        ),
                        'https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling',
                        jsonb_build_object(
                            'value',              'False',
                            'reason',             'Idle',
                            'lastTransitionTime', e.event -> 'event_time'
                        )
                    )
                )
            ELSE f.status
        END
    FROM fold f
    JOIN resource_events_ordered e
        ON e.resource_id = f.resource_id
       AND e.seq = f.seq + 1
)
SELECT * FROM fold;

/* ------------------------------ */

DO $$
DECLARE
    mismatched_resource_id UUID;
BEGIN
    SELECT r.resource_id INTO mismatched_resource_id
    FROM resources r
    JOIN (
        SELECT DISTINCT ON (resource_id) resource_id, status
        FROM resource_status_fold
        ORDER BY resource_id, seq DESC
    ) final ON final.resource_id = r.resource_id
    WHERE pg_temp.resource_status_semantics(final.status)
          IS DISTINCT FROM pg_temp.resource_status_semantics(
              COALESCE(r.status, '{"phase": "Pending", "conditions": {}}'::jsonb)
          )
    LIMIT 1;

    IF mismatched_resource_id IS NOT NULL THEN
        RAISE EXCEPTION
            'Folding the stored events of resource % does not reproduce its status',
            mismatched_resource_id;
    END IF;
END $$;

/* ------------------------------ */

UPDATE resource_events re
SET
    event_type    = 'StatusUpdated',
    event_payload = jsonb_build_object('StatusUpdated', jsonb_build_object(
        'event_time', f.event -> 'event_time',
        'id',         f.event -> 'id',
        'new_status', f.status
    ))
FROM resource_status_fold f
WHERE re.event_id = f.event_id
  AND f.event_type IN (
        'ReconciliationStarted',
        'ReconciliationSucceeded',
        'ReconciliationFailed'
    );

/* ------------------------------ */

UPDATE resources r
SET
    status = CASE
        WHEN derived.last_status IS NULL THEN r.status
        WHEN COALESCE((derived.last_status ->> 'observedGeneration')::BIGINT, 0) < r.generation
            THEN derived.last_status || '{"phase": "Pending"}'::jsonb
        ELSE derived.last_status
    END,
    status_generation = 1 + derived.status_updates,
    updated_at        = derived.last_event_time
FROM (
    SELECT
        resource_id,
        COUNT(*) FILTER (WHERE event_type = 'StatusUpdated') AS status_updates,
        (ARRAY_AGG(event_time ORDER BY event_id DESC))[1]    AS last_event_time,
        (
            ARRAY_AGG(event_payload -> 'StatusUpdated' -> 'new_status' ORDER BY event_id DESC)
            FILTER (WHERE event_type = 'StatusUpdated')
        )[1]                                                 AS last_status
    FROM resource_events
    GROUP BY resource_id
) derived
WHERE r.resource_id = derived.resource_id;

/* ------------------------------ */

DROP FUNCTION pg_temp.set_resource_condition(JSONB, TEXT, JSONB);
DROP FUNCTION pg_temp.resource_status_semantics(JSONB);
