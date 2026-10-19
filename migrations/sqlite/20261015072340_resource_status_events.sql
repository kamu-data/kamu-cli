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
 *
 * Mirrors the Postgres migration of the same name, which reads more easily:
 * SQLite has no functions, so the condition update is spelled out per use.
 */

ALTER TABLE resources ADD COLUMN status_generation BIGINT NOT NULL DEFAULT 1;

/* ------------------------------ */

CREATE TEMP TABLE resource_events_ordered AS
SELECT
    event_id,
    resource_id,
    event_type,
    json_extract(event_payload, '$.' || event_type) AS event,
    ROW_NUMBER() OVER (PARTITION BY resource_id ORDER BY event_id) AS seq
FROM resource_events;

CREATE UNIQUE INDEX temp.resource_events_ordered_seq ON resource_events_ordered (resource_id, seq);

/* ------------------------------ */

CREATE TEMP TABLE resource_status_fold AS
WITH RECURSIVE fold AS (
    SELECT
        e.resource_id,
        e.seq,
        e.event_id,
        e.event_type,
        e.event,
        json('{"phase": "Pending", "conditions": {}}') AS status
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
                json_set(f.status, '$.phase', 'Pending', '$.conditions', json('{}'))
            WHEN 'ReconciliationStarted' THEN
                json_set(
                    f.status,
                    '$.phase',              'Reconciling',
                    '$.observedGeneration', json_extract(e.event, '$.generation'),
                    '$.observedAt',         json_extract(e.event, '$.event_time'),
                    '$.conditions',         json(json_set(
                        json_extract(f.status, '$.conditions'),
                        '$."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling"',
                        json(json_object(
                            'value',              'True',
                            'reason',             'Processing',
                            'lastTransitionTime', CASE
                                WHEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling".value') = 'True'
                                    THEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling".lastTransitionTime')
                                ELSE json_extract(e.event, '$.event_time')
                            END
                        ))
                    ))
                )
            WHEN 'ReconciliationSucceeded' THEN
                json_set(
                    f.status,
                    '$.phase',                'Ready',
                    '$.reconciledGeneration', json_extract(e.event, '$.generation'),
                    '$.reconciledAt',         json_extract(e.event, '$.event_time'),
                    '$.conditions',           json(json_set(
                        json_extract(f.status, '$.conditions'),
                        '$."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready"',
                        json(json_object(
                            'value',              'True',
                            'reason',             'Reconciled',
                            'lastTransitionTime', CASE
                                WHEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".value') = 'True'
                                    THEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".lastTransitionTime')
                                ELSE json_extract(e.event, '$.event_time')
                            END
                        )),
                        '$."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling"',
                        json(json_object(
                            'value',              'False',
                            'reason',             'Idle',
                            'lastTransitionTime', CASE
                                WHEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling".value') = 'False'
                                    THEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling".lastTransitionTime')
                                ELSE json_extract(e.event, '$.event_time')
                            END
                        ))
                    ))
                )
            WHEN 'ReconciliationFailed' THEN
                json_set(
                    f.status,
                    '$.phase',      CASE
                                        WHEN json_type(f.status, '$.reconciledGeneration') IS NOT NULL
                                            THEN 'Degraded'
                                        ELSE 'Failed'
                                    END,
                    '$.conditions', json(json_set(
                        json_extract(f.status, '$.conditions'),
                        '$."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready"',
                        json(json_object(
                            'value',              'False',
                            'reason',             json_extract(e.event, '$.reason'),
                            'message',            json_extract(e.event, '$.message'),
                            'lastTransitionTime', CASE
                                WHEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".value') = 'False'
                                    THEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready".lastTransitionTime')
                                ELSE json_extract(e.event, '$.event_time')
                            END
                        )),
                        '$."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling"',
                        json(json_object(
                            'value',              'False',
                            'reason',             'Idle',
                            'lastTransitionTime', CASE
                                WHEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling".value') = 'False'
                                    THEN json_extract(f.status, '$.conditions."https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling".lastTransitionTime')
                                ELSE json_extract(e.event, '$.event_time')
                            END
                        ))
                    ))
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

/*
 * Guard: the final folded status of each resource must match the stored one.
 * Timestamps are left out, as their text form varies. Condition fields are
 * picked out by name, since SQLite compares JSON as text and key order varies. A mismatch fails the
 * CHECK constraint below, which aborts the migration.
 */
CREATE TEMP TABLE resource_status_check AS
SELECT
    r.resource_id,
    final.status                                                      AS folded,
    COALESCE(r.status, json('{"phase": "Pending", "conditions": {}}')) AS stored
FROM resources r
JOIN resource_status_fold final
    ON final.resource_id = r.resource_id
   AND final.seq = (
        SELECT MAX(seq) FROM resource_status_fold f WHERE f.resource_id = r.resource_id
   );

CREATE TEMP TABLE resource_status_semantics AS
SELECT
    resource_id,
    side,
    json_object(
        'phase',                json_extract(status, '$.phase'),
        'observedGeneration',   json_extract(status, '$.observedGeneration'),
        'reconciledGeneration', json_extract(status, '$.reconciledGeneration'),
        'conditions',           json((
            SELECT json_group_object(
                key,
                json_object(
                    'value',   json_extract(value, '$.value'),
                    'reason',  json_extract(value, '$.reason'),
                    'message', json_extract(value, '$.message')
                )
            )
            FROM (SELECT key, value FROM json_each(status, '$.conditions') ORDER BY key)
        ))
    ) AS semantics
FROM (
    SELECT resource_id, 'folded' AS side, folded AS status FROM resource_status_check
    UNION ALL
    SELECT resource_id, 'stored' AS side, stored AS status FROM resource_status_check
);

CREATE TEMP TABLE resource_status_guard (
    resources_whose_events_do_not_reproduce_their_status INTEGER NOT NULL
        CHECK (resources_whose_events_do_not_reproduce_their_status = 0)
);

INSERT INTO resource_status_guard
SELECT COUNT(*)
FROM resource_status_semantics folded
JOIN resource_status_semantics stored
    ON stored.resource_id = folded.resource_id
   AND stored.side = 'stored'
WHERE folded.side = 'folded'
  AND folded.semantics IS NOT stored.semantics;

/* ------------------------------ */

UPDATE resource_events
SET
    event_type    = 'StatusUpdated',
    event_payload = json_object('StatusUpdated', json_object(
        'event_time', json_extract(f.event, '$.event_time'),
        'id',         json_extract(f.event, '$.id'),
        'new_status', json(f.status)
    ))
FROM resource_status_fold f
WHERE resource_events.event_id = f.event_id
  AND f.event_type IN (
        'ReconciliationStarted',
        'ReconciliationSucceeded',
        'ReconciliationFailed'
    );

/* ------------------------------ */

UPDATE resources
SET
    status = CASE
        WHEN derived.last_status IS NULL THEN resources.status
        WHEN COALESCE(json_extract(derived.last_status, '$.observedGeneration'), 0) < resources.generation
            THEN json_set(derived.last_status, '$.phase', 'Pending')
        ELSE json(derived.last_status)
    END,
    status_generation = 1 + derived.status_updates,
    updated_at        = derived.last_event_time
FROM (
    SELECT
        e.resource_id,
        SUM(e.event_type = 'StatusUpdated') AS status_updates,
        (
            SELECT t.event_time
            FROM resource_events t
            WHERE t.resource_id = e.resource_id
            ORDER BY t.event_id DESC
            LIMIT 1
        )                                   AS last_event_time,
        (
            SELECT json_extract(s.event_payload, '$.StatusUpdated.new_status')
            FROM resource_events s
            WHERE s.resource_id = e.resource_id
              AND s.event_type = 'StatusUpdated'
            ORDER BY s.event_id DESC
            LIMIT 1
        )                                   AS last_status
    FROM resource_events e
    GROUP BY e.resource_id
) derived
WHERE resources.resource_id = derived.resource_id;

/* ------------------------------ */

DROP TABLE temp.resource_status_guard;
DROP TABLE temp.resource_status_semantics;
DROP TABLE temp.resource_status_check;
DROP TABLE temp.resource_status_fold;
DROP TABLE temp.resource_events_ordered;
