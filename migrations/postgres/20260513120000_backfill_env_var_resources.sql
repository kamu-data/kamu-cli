-- Backfill: promote legacy dataset_env_vars rows into managed VariableSet / SecretSet
-- resources so the new DatasetEnvVarResolver can serve them through bindings.
--
-- Each dataset that has at least one plaintext env var gets exactly one VariableSet
-- resource named  "legacy-vars-<dataset_id>" owned by the dataset owner.
-- Each dataset that has at least one secret env var gets exactly one SecretSet
-- resource named "legacy-secrets-<dataset_id>" owned by the dataset owner.
--
-- For both resource types we also create:
--   * A generation-1 projection entry row per variable / secret.
--   * A binding row connecting the dataset to the resource at order 0.
--
-- The migration is idempotent: ON CONFLICT DO NOTHING guards every INSERT.

/* ------------------------------ */
/* VariableSet resources          */
/* ------------------------------ */

INSERT INTO resources (
    resource_uid,
    account_id,
    resource_kind,
    api_version,
    resource_name,
    description,
    labels,
    annotations,
    spec,
    status,
    generation,
    created_at,
    updated_at,
    deleted_at,
    last_reconciled_at,
    last_event_id
)
SELECT
    gen_random_uuid()                                                           AS resource_uid,
    de.owner_id                                                                 AS account_id,
    'VariableSet'                                                               AS resource_kind,
    'kamu.dev/v1alpha1'                                                         AS api_version,
    'legacy-vars-' || substring(dev.dataset_id, 9)                              AS resource_name,
    NULL                                                                        AS description,
    '{}'::jsonb                                                                 AS labels,
    '{}'::jsonb                                                                 AS annotations,
    jsonb_build_object(
        'variables',
        (
            SELECT jsonb_object_agg(d2.key, convert_from(d2.value, 'UTF8'))
            FROM dataset_env_vars d2
            WHERE d2.dataset_id = dev.dataset_id
              AND d2.secret_nonce IS NULL
        )
    )                                                                           AS spec,
    jsonb_build_object(
        'phase', 'Ready',
        'stats', jsonb_build_object(
            'totalVariables', COUNT(*),
            'validVariables', COUNT(*),
            'invalidVariables', 0
        )
    )                                                                           AS status,
    1                                                                           AS generation,
    MIN(dev.created_at)                                                         AS created_at,
    MIN(dev.created_at)                                                         AS updated_at,
    NULL                                                                        AS deleted_at,
    MIN(dev.created_at)                                                         AS last_reconciled_at,
    NULL                                                                        AS last_event_id
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
WHERE dev.secret_nonce IS NULL
GROUP BY dev.dataset_id, de.owner_id
ON CONFLICT (account_id, resource_kind, resource_name) DO NOTHING;

/* ------------------------------ */

INSERT INTO config_variable_set_entries (
    entry_id,
    resource_uid,
    resource_generation,
    account_id,
    variable_key,
    value,
    created_at,
    updated_at
)
SELECT
    gen_random_uuid()                                                           AS entry_id,
    r.resource_uid,
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
   AND r.resource_kind = 'VariableSet'
   AND r.resource_name = 'legacy-vars-' || substring(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NULL
ON CONFLICT (resource_uid, resource_generation, variable_key) DO NOTHING;

/* ------------------------------ */

INSERT INTO config_dataset_variable_set_bindings (
    dataset_id,
    resource_uid,
    binding_order
)
SELECT DISTINCT
    dev.dataset_id,
    r.resource_uid,
    0                                                                           AS binding_order
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
JOIN resources r
    ON r.account_id = de.owner_id
   AND r.resource_kind = 'VariableSet'
   AND r.resource_name = 'legacy-vars-' || substring(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NULL
ON CONFLICT DO NOTHING;

/* ------------------------------ */
/* SecretSet resources            */
/* ------------------------------ */

INSERT INTO resources (
    resource_uid,
    account_id,
    resource_kind,
    api_version,
    resource_name,
    description,
    labels,
    annotations,
    spec,
    status,
    generation,
    created_at,
    updated_at,
    deleted_at,
    last_reconciled_at,
    last_event_id
)
SELECT
    gen_random_uuid()                                                           AS resource_uid,
    de.owner_id                                                                 AS account_id,
    'SecretSet'                                                                 AS resource_kind,
    'kamu.dev/v1alpha1'                                                         AS api_version,
    'legacy-secrets-' || substring(dev.dataset_id, 9)                           AS resource_name,
    NULL                                                                        AS description,
    '{}'::jsonb                                                                 AS labels,
    '{}'::jsonb                                                                 AS annotations,
    jsonb_build_object(
        'secrets',
        (
            SELECT jsonb_object_agg(d2.key, '<migrated>')
            FROM dataset_env_vars d2
            WHERE d2.dataset_id = dev.dataset_id
              AND d2.secret_nonce IS NOT NULL
        )
    )                                                                           AS spec,
    jsonb_build_object(
        'phase', 'Ready',
        'stats', jsonb_build_object(
            'totalSecrets', COUNT(*),
            'validSecrets', COUNT(*),
            'invalidSecrets', 0
        )
    )                                                                           AS status,
    1                                                                           AS generation,
    MIN(dev.created_at)                                                         AS created_at,
    MIN(dev.created_at)                                                         AS updated_at,
    NULL                                                                        AS deleted_at,
    MIN(dev.created_at)                                                         AS last_reconciled_at,
    NULL                                                                        AS last_event_id
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
WHERE dev.secret_nonce IS NOT NULL
GROUP BY dev.dataset_id, de.owner_id
ON CONFLICT (account_id, resource_kind, resource_name) DO NOTHING;

/* ------------------------------ */

INSERT INTO config_secret_set_entries (
    entry_id,
    resource_uid,
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
    r.resource_uid,
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
   AND r.resource_kind = 'SecretSet'
   AND r.resource_name = 'legacy-secrets-' || substring(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NOT NULL
ON CONFLICT (resource_uid, resource_generation, secret_key) DO NOTHING;

/* ------------------------------ */

INSERT INTO config_dataset_secret_set_bindings (
    dataset_id,
    resource_uid,
    binding_order
)
SELECT DISTINCT
    dev.dataset_id,
    r.resource_uid,
    0                                                                           AS binding_order
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
JOIN resources r
    ON r.account_id = de.owner_id
   AND r.resource_kind = 'SecretSet'
   AND r.resource_name = 'legacy-secrets-' || substring(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NOT NULL
ON CONFLICT DO NOTHING;

/* ------------------------------ */
/* Resource events: VariableSet   */
/* ------------------------------ */

WITH
var_resources AS (
    SELECT resource_uid, account_id, resource_name, created_at, spec, status
    FROM resources
    WHERE resource_kind = 'VariableSet'
      AND resource_name LIKE 'legacy-vars-%'
      AND last_event_id IS NULL
),
ins_created AS (
    INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
    SELECT
        r.resource_uid,
        'VariableSet',
        r.created_at,
        'Created',
        jsonb_build_object('Created', jsonb_build_object(
            'event_time', r.created_at,
            'uid',         r.resource_uid::text,
            'metadata',    jsonb_build_object(
                               'account',     r.account_id,
                               'name',        r.resource_name,
                               'description', NULL,
                               'labels',      '{}'::jsonb,
                               'annotations', '{}'::jsonb
                           ),
            'spec',        r.spec
        ))
    FROM var_resources r
    RETURNING resource_uid, event_id
),
ins_started AS (
    INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
    SELECT
        r.resource_uid,
        'VariableSet',
        r.created_at,
        'ReconciliationStarted',
        jsonb_build_object('ReconciliationStarted', jsonb_build_object(
            'event_time', r.created_at,
            'uid',        r.resource_uid::text,
            'generation', 1
        ))
    FROM var_resources r
    RETURNING resource_uid, event_id
),
ins_succeeded AS (
    INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
    SELECT
        r.resource_uid,
        'VariableSet',
        r.created_at,
        'ReconciliationSucceeded',
        jsonb_build_object('ReconciliationSucceeded', jsonb_build_object(
            'event_time', r.created_at,
            'uid',        r.resource_uid::text,
            'generation', 1,
            'success',    jsonb_build_object('stats', r.status -> 'stats')
        ))
    FROM var_resources r
    RETURNING resource_uid, event_id
)
UPDATE resources
SET last_event_id = ins_succeeded.event_id
FROM ins_succeeded
WHERE resources.resource_uid = ins_succeeded.resource_uid;

/* ------------------------------ */
/* Resource events: SecretSet     */
/* ------------------------------ */

WITH
secret_resources AS (
    SELECT resource_uid, account_id, resource_name, created_at, spec, status
    FROM resources
    WHERE resource_kind = 'SecretSet'
      AND resource_name LIKE 'legacy-secrets-%'
      AND last_event_id IS NULL
),
ins_created AS (
    INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
    SELECT
        r.resource_uid,
        'SecretSet',
        r.created_at,
        'Created',
        jsonb_build_object('Created', jsonb_build_object(
            'event_time', r.created_at,
            'uid',         r.resource_uid::text,
            'metadata',    jsonb_build_object(
                               'account',     r.account_id,
                               'name',        r.resource_name,
                               'description', NULL,
                               'labels',      '{}'::jsonb,
                               'annotations', '{}'::jsonb
                           ),
            'spec',        r.spec
        ))
    FROM secret_resources r
    RETURNING resource_uid, event_id
),
ins_started AS (
    INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
    SELECT
        r.resource_uid,
        'SecretSet',
        r.created_at,
        'ReconciliationStarted',
        jsonb_build_object('ReconciliationStarted', jsonb_build_object(
            'event_time', r.created_at,
            'uid',        r.resource_uid::text,
            'generation', 1
        ))
    FROM secret_resources r
    RETURNING resource_uid, event_id
),
ins_succeeded AS (
    INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
    SELECT
        r.resource_uid,
        'SecretSet',
        r.created_at,
        'ReconciliationSucceeded',
        jsonb_build_object('ReconciliationSucceeded', jsonb_build_object(
            'event_time', r.created_at,
            'uid',        r.resource_uid::text,
            'generation', 1,
            'success',    jsonb_build_object('stats', r.status -> 'stats')
        ))
    FROM secret_resources r
    RETURNING resource_uid, event_id
)
UPDATE resources
SET last_event_id = ins_succeeded.event_id
FROM ins_succeeded
WHERE resources.resource_uid = ins_succeeded.resource_uid;

/* ------------------------------ */

/* Drop legacy table — all data has been promoted to resources above */
DROP TABLE dataset_env_vars;

/* ------------------------------ */
