-- Backfill: promote legacy dataset_env_vars rows into managed VariableSet / SecretSet
-- resources so the new DatasetEnvVarResolver can serve them through the
-- `legacy-config-target-dataset` label.
--
-- Each dataset that has at least one plaintext env var gets exactly one VariableSet
-- resource named  "legacy-vars-<dataset_id>" owned by the dataset owner.
-- Each dataset that has at least one secret env var gets exactly one SecretSet
-- resource named "legacy-secrets-<dataset_id>" owned by the dataset owner.
--
-- For both resource types we also create:
--   * A generation-1 projection entry row per variable / secret.
--   * A `legacy-config-target-dataset` label carrying the dataset DID, which
--     is what associates the resource with its dataset.
--
-- The migration is idempotent: ON CONFLICT DO NOTHING guards every INSERT.

/* ------------------------------ */
/* VariableSet resources          */
/* ------------------------------ */

INSERT INTO resources (
    resource_id,
    account_id,
    resource_schema,
    resource_name,
    labels,
    annotations,
    spec,
    status,
    generation,
    created_at,
    updated_at,
    deleted_at,
    last_event_id
)
SELECT
    gen_random_uuid()                                                           AS resource_id,
    de.owner_id                                                                 AS account_id,
    'https://opendatafabric.org/schemas/config/v1alpha1/VariableSet'                 AS resource_schema,
    'legacy-vars-' || substring(dev.dataset_id, 9)                              AS resource_name,
    jsonb_build_object(
        'https://kamu.dev/schemas/resource/v1alpha1/labels/LegacyConfigTargetDataset',
        dev.dataset_id
    )                                                                           AS labels,
    '{}'::jsonb                                                                 AS annotations,
    jsonb_build_object(
        'variables',
        (
            SELECT jsonb_object_agg(d2.key, jsonb_build_object('value', convert_from(d2.value, 'UTF8')))
            FROM dataset_env_vars d2
            WHERE d2.dataset_id = dev.dataset_id
              AND d2.secret_nonce IS NULL
        )
    )                                                                           AS spec,
    jsonb_build_object(
        'phase', 'Ready',
        'observedGeneration', 1,
        'reconciledAt', MIN(dev.created_at),
        'conditions', jsonb_build_object(
            'https://kamu.dev/schemas/resource/v1alpha1/conditions/Accepted',
            jsonb_build_object(
                'status', 'True',
                'reason', 'ValidationPassed',
                'lastTransitionTime', MIN(dev.created_at)
            ),
            'https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready',
            jsonb_build_object(
                'status', 'True',
                'reason', 'Reconciled',
                'lastTransitionTime', MIN(dev.created_at)
            ),
            'https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling',
            jsonb_build_object(
                'status', 'False',
                'reason', 'Idle',
                'lastTransitionTime', MIN(dev.created_at)
            )
        )
    )                                                                           AS status,
    1                                                                           AS generation,
    MIN(dev.created_at)                                                         AS created_at,
    MIN(dev.created_at)                                                         AS updated_at,
    NULL                                                                        AS deleted_at,
    NULL                                                                        AS last_event_id
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
WHERE dev.secret_nonce IS NULL
GROUP BY dev.dataset_id, de.owner_id
ON CONFLICT (account_id, resource_schema, LOWER(resource_name)) DO NOTHING;

/* ------------------------------ */

INSERT INTO config_variable_set_entries (
    entry_id,
    resource_id,
    resource_generation,
    account_id,
    variable_key,
    value,
    created_at,
    updated_at
)
SELECT
    gen_random_uuid()                                                           AS entry_id,
    r.resource_id,
    1                                                                           AS resource_generation,
    de.owner_id                                                                 AS account_id,
    dev.key                                                                     AS variable_key,
    convert_from(dev.value, 'UTF8')                                             AS value,
    dev.created_at,
    dev.created_at                                                              AS updated_at
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
JOIN resources r
    ON r.account_id = de.owner_id
   AND r.resource_schema = 'https://opendatafabric.org/schemas/config/v1alpha1/VariableSet'
   AND r.resource_name = 'legacy-vars-' || substring(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NULL
ON CONFLICT (resource_id, resource_generation, variable_key) DO NOTHING;

/* ------------------------------ */
/* SecretSet resources            */
/* ------------------------------ */

INSERT INTO resources (
    resource_id,
    account_id,
    resource_schema,
    resource_name,
    labels,
    annotations,
    spec,
    status,
    generation,
    created_at,
    updated_at,
    deleted_at,
    last_event_id
)
SELECT
    gen_random_uuid()                                                           AS resource_id,
    de.owner_id                                                                 AS account_id,
    'https://opendatafabric.org/schemas/config/v1alpha1/SecretSet'                   AS resource_schema,
    'legacy-secrets-' || substring(dev.dataset_id, 9)                           AS resource_name,
    jsonb_build_object(
        'https://kamu.dev/schemas/resource/v1alpha1/labels/LegacyConfigTargetDataset',
        dev.dataset_id
    )                                                                           AS labels,
    '{}'::jsonb                                                                 AS annotations,
    jsonb_build_object(
        'secrets',
        (
            -- Emit the legacy secret in the RFC-18 shape using the read-only
            -- `aes256gcm` encoding: hex(nonce ‖ ciphertext). SQL cannot produce a
            -- JWE token, so the node reads this legacy form and re-materializes on
            -- the next apply. See SecretExt::decrypt_plaintext_bytes.
            SELECT jsonb_object_agg(
                d2.key,
                jsonb_build_object(
                    'value', encode(d2.secret_nonce || d2.value, 'hex'),
                    'contentEncoding', 'aes256gcm'
                )
            )
            FROM dataset_env_vars d2
            WHERE d2.dataset_id = dev.dataset_id
              AND d2.secret_nonce IS NOT NULL
        )
    )                                                                           AS spec,
    jsonb_build_object(
        'phase', 'Ready',
        'observedGeneration', 1,
        'reconciledAt', MIN(dev.created_at),
        'conditions', jsonb_build_object(
            'https://kamu.dev/schemas/resource/v1alpha1/conditions/Accepted',
            jsonb_build_object(
                'status', 'True',
                'reason', 'ValidationPassed',
                'lastTransitionTime', MIN(dev.created_at)
            ),
            'https://kamu.dev/schemas/resource/v1alpha1/conditions/Ready',
            jsonb_build_object(
                'status', 'True',
                'reason', 'Reconciled',
                'lastTransitionTime', MIN(dev.created_at)
            ),
            'https://kamu.dev/schemas/resource/v1alpha1/conditions/Reconciling',
            jsonb_build_object(
                'status', 'False',
                'reason', 'Idle',
                'lastTransitionTime', MIN(dev.created_at)
            )
        )
    )                                                                           AS status,
    1                                                                           AS generation,
    MIN(dev.created_at)                                                         AS created_at,
    MIN(dev.created_at)                                                         AS updated_at,
    NULL                                                                        AS deleted_at,
    NULL                                                                        AS last_event_id
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
WHERE dev.secret_nonce IS NOT NULL
GROUP BY dev.dataset_id, de.owner_id
ON CONFLICT (account_id, resource_schema, LOWER(resource_name)) DO NOTHING;

/* ------------------------------ */

INSERT INTO config_secret_set_entries (
    entry_id,
    resource_id,
    resource_generation,
    account_id,
    secret_key,
    value,
    secret_nonce,
    created_at,
    updated_at
)
SELECT
    gen_random_uuid()                                                           AS entry_id,
    r.resource_id,
    1                                                                           AS resource_generation,
    de.owner_id                                                                 AS account_id,
    dev.key                                                                     AS secret_key,
    dev.value,
    dev.secret_nonce,
    dev.created_at,
    dev.created_at                                                              AS updated_at
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
JOIN resources r
    ON r.account_id = de.owner_id
   AND r.resource_schema = 'https://opendatafabric.org/schemas/config/v1alpha1/SecretSet'
   AND r.resource_name = 'legacy-secrets-' || substring(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NOT NULL
ON CONFLICT (resource_id, resource_generation, secret_key) DO NOTHING;

/* ------------------------------ */
/* Resource events                */
/* ------------------------------ */

/*
 * The three lifecycle events per resource must receive `event_id`s in causal
 * order: the event store replays strictly `ORDER BY event_id`, and the state
 * projection can only be initialized from `Created`. If `ReconciliationSucceeded`
 * lands on a lower id, loading the resource fails with
 * "Cannot initialize ... from event ReconciliationSucceeded", making every
 * backfilled resource unreadable and unwritable.
 *
 * An earlier version emitted the three events from sibling data-modifying CTEs.
 * Postgres does not guarantee the execution order of such CTEs, so `nextval` was
 * free to number them in any order -- and in practice numbered them backwards.
 * They are emitted here as a single INSERT whose ORDER BY fixes the sequence
 * assignment explicitly, which is both deterministic and cheaper than three
 * separate statements.
 */

INSERT INTO resource_events (resource_id, resource_schema, event_time, event_type, event_payload)
SELECT
    r.resource_id,
    r.resource_schema,
    r.created_at,
    e.event_type,
    CASE e.ord
        WHEN 1 THEN jsonb_build_object('Created', jsonb_build_object(
            'event_time', r.created_at,
            'id',         r.resource_id::text,
            'headers',    jsonb_build_object(
                               -- `AccountRef` is an object, not a bare DID. A
                               -- string here deserializes into a ref carrying
                               -- only `did`, and applying a manifest over such
                               -- a resource then fails to load it. Mirror what
                               -- the runtime writes: id (account resource UUID),
                               -- did and name.
                               'account',     jsonb_build_object(
                                                  'id',   acc.resource_id::text,
                                                  'did',  acc.id,
                                                  'name', acc.account_name
                                              ),
                               'name',        r.resource_name,
                               'labels',      r.labels,
                               'annotations', '{}'::jsonb
                           ),
            'spec',       r.spec
        ))
        WHEN 2 THEN jsonb_build_object('ReconciliationStarted', jsonb_build_object(
            'event_time', r.created_at,
            'id',         r.resource_id::text,
            'generation', 1
        ))
        ELSE jsonb_build_object('ReconciliationSucceeded', jsonb_build_object(
            'event_time', r.created_at,
            'id',         r.resource_id::text,
            'generation', 1,
            'success',    jsonb_build_object()
        ))
    END
FROM resources r
JOIN accounts acc ON acc.id = r.account_id
CROSS JOIN (VALUES
    (1, 'Created'),
    (2, 'ReconciliationStarted'),
    (3, 'ReconciliationSucceeded')
) AS e(ord, event_type)
WHERE r.resource_schema IN (
        'https://opendatafabric.org/schemas/config/v1alpha1/VariableSet',
        'https://opendatafabric.org/schemas/config/v1alpha1/SecretSet')
  AND (r.resource_name LIKE 'legacy-vars-%' OR r.resource_name LIKE 'legacy-secrets-%')
  AND r.last_event_id IS NULL
ORDER BY r.resource_id, e.ord;

/* ------------------------------ */

/* Point each resource at its terminal (ReconciliationSucceeded) event. */
UPDATE resources
SET last_event_id = last_events.event_id
FROM (
    SELECT resource_id, MAX(event_id) AS event_id
    FROM resource_events
    GROUP BY resource_id
) AS last_events
WHERE resources.resource_id = last_events.resource_id
  AND resources.last_event_id IS NULL
  AND (resources.resource_name LIKE 'legacy-vars-%'
    OR resources.resource_name LIKE 'legacy-secrets-%');

/* ------------------------------ */

/* Drop legacy table — all data has been promoted to resources above */
DROP TABLE dataset_env_vars;

/* ------------------------------ */
