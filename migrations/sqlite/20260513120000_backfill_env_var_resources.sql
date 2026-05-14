-- Backfill: promote legacy dataset_env_vars rows into managed VariableSet / SecretSet
-- resources so the new DatasetEnvVarResolver can serve them through bindings.
--
-- SQLite variant – uses lower(hex(randomblob(N))) for UUID generation and
-- json_object / json_group_object for JSON construction.
--
-- The migration is idempotent: ON CONFLICT DO NOTHING guards every INSERT.

/* ------------------------------ */
/* VariableSet resources          */
/* ------------------------------ */

INSERT OR IGNORE INTO resources (
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
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        substr(lower(hex(randomblob(2))),2) || '-' ||
        substr('89ab', abs(random()) % 4 + 1, 1) ||
        substr(lower(hex(randomblob(2))),2) || '-' ||
        lower(hex(randomblob(6)))                                               AS resource_uid,
    de.owner_id                                                                 AS account_id,
    'VariableSet'                                                               AS resource_kind,
    'kamu.dev/v1alpha1'                                                         AS api_version,
    'legacy-vars-' || substr(dev.dataset_id, 9)                                 AS resource_name,
    NULL                                                                        AS description,
    '{}'                                                                        AS labels,
    '{}'                                                                        AS annotations,
    json_object(
        'variables',
        (
            SELECT json_group_object(d2.key, CAST(d2.value AS TEXT))
            FROM dataset_env_vars d2
            WHERE d2.dataset_id = dev.dataset_id
              AND d2.secret_nonce IS NULL
        )
    )                                                                           AS spec,
    json_object(
        'phase', 'Ready',
        'stats', json_object(
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
GROUP BY dev.dataset_id, de.owner_id;

/* ------------------------------ */

INSERT OR IGNORE INTO config_variable_set_entries (
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
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        substr(lower(hex(randomblob(2))),2) || '-' ||
        substr('89ab', abs(random()) % 4 + 1, 1) ||
        substr(lower(hex(randomblob(2))),2) || '-' ||
        lower(hex(randomblob(6)))                                               AS entry_id,
    r.resource_uid,
    1                                                                           AS resource_generation,
    de.owner_id                                                                 AS account_id,
    dev.key                                                                     AS variable_key,
    CAST(dev.value AS TEXT)                                                     AS value,
    dev.created_at,
    dev.created_at                                                              AS updated_at
FROM dataset_env_vars dev
JOIN dataset_entries de ON de.dataset_id = dev.dataset_id
JOIN resources r
    ON r.account_id = de.owner_id
   AND r.resource_kind = 'VariableSet'
   AND r.resource_name = 'legacy-vars-' || substr(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NULL;

/* ------------------------------ */

INSERT OR IGNORE INTO config_dataset_variable_set_bindings (
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
   AND r.resource_name = 'legacy-vars-' || substr(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NULL;

/* ------------------------------ */
/* SecretSet resources            */
/* ------------------------------ */

INSERT OR IGNORE INTO resources (
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
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        substr(lower(hex(randomblob(2))),2) || '-' ||
        substr('89ab', abs(random()) % 4 + 1, 1) ||
        substr(lower(hex(randomblob(2))),2) || '-' ||
        lower(hex(randomblob(6)))                                               AS resource_uid,
    de.owner_id                                                                 AS account_id,
    'SecretSet'                                                                 AS resource_kind,
    'kamu.dev/v1alpha1'                                                         AS api_version,
    'legacy-secrets-' || substr(dev.dataset_id, 9)                              AS resource_name,
    NULL                                                                        AS description,
    '{}'                                                                        AS labels,
    '{}'                                                                        AS annotations,
    json_object(
        'secrets',
        (
            SELECT json_group_object(d2.key, '<migrated>')
            FROM dataset_env_vars d2
            WHERE d2.dataset_id = dev.dataset_id
              AND d2.secret_nonce IS NOT NULL
        )
    )                                                                           AS spec,
    json_object(
        'phase', 'Ready',
        'stats', json_object(
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
GROUP BY dev.dataset_id, de.owner_id;

/* ------------------------------ */

INSERT OR IGNORE INTO config_secret_set_entries (
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
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        substr(lower(hex(randomblob(2))),2) || '-' ||
        substr('89ab', abs(random()) % 4 + 1, 1) ||
        substr(lower(hex(randomblob(2))),2) || '-' ||
        lower(hex(randomblob(6)))                                               AS entry_id,
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
   AND r.resource_name = 'legacy-secrets-' || substr(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NOT NULL;

/* ------------------------------ */

INSERT OR IGNORE INTO config_dataset_secret_set_bindings (
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
   AND r.resource_name = 'legacy-secrets-' || substr(dev.dataset_id, 9)
WHERE dev.secret_nonce IS NOT NULL;

/* ------------------------------ */
/* Resource events: VariableSet   */
/* ------------------------------ */

INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
SELECT
    r.resource_uid,
    'VariableSet',
    r.created_at,
    'Created',
    '{"Created":{"event_time":"' || r.created_at || '","uid":"' || r.resource_uid
        || '","metadata":{"account":"' || r.account_id
        || '","name":"' || r.resource_name
        || '","description":null,"labels":{},"annotations":{}},"spec":' || r.spec || '}}'
FROM resources r
WHERE r.resource_kind = 'VariableSet'
  AND r.resource_name LIKE 'legacy-vars-%'
  AND r.last_event_id IS NULL;

INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
SELECT
    r.resource_uid,
    'VariableSet',
    r.created_at,
    'ReconciliationStarted',
    json_object(
        'ReconciliationStarted', json_object(
            'event_time', r.created_at,
            'uid',        r.resource_uid,
            'generation', 1
        )
    )
FROM resources r
WHERE r.resource_kind = 'VariableSet'
  AND r.resource_name LIKE 'legacy-vars-%'
  AND r.last_event_id IS NULL;

INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
SELECT
    r.resource_uid,
    'VariableSet',
    r.created_at,
    'ReconciliationSucceeded',
    '{"ReconciliationSucceeded":{"event_time":"' || r.created_at || '","uid":"' || r.resource_uid
        || '","generation":1,"success":{"stats":' || json_extract(r.status, '$.stats') || '}}}'
FROM resources r
WHERE r.resource_kind = 'VariableSet'
  AND r.resource_name LIKE 'legacy-vars-%'
  AND r.last_event_id IS NULL;

UPDATE resources
SET last_event_id = (
    SELECT MAX(e.event_id)
    FROM resource_events e
    WHERE e.resource_uid   = resources.resource_uid
      AND e.resource_kind  = 'VariableSet'
      AND e.event_type     = 'ReconciliationSucceeded'
)
WHERE resource_kind  = 'VariableSet'
  AND resource_name  LIKE 'legacy-vars-%'
  AND last_event_id  IS NULL;

/* ------------------------------ */
/* Resource events: SecretSet     */
/* ------------------------------ */

INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
SELECT
    r.resource_uid,
    'SecretSet',
    r.created_at,
    'Created',
    '{"Created":{"event_time":"' || r.created_at || '","uid":"' || r.resource_uid
        || '","metadata":{"account":"' || r.account_id
        || '","name":"' || r.resource_name
        || '","description":null,"labels":{},"annotations":{}},"spec":' || r.spec || '}}'
FROM resources r
WHERE r.resource_kind = 'SecretSet'
  AND r.resource_name LIKE 'legacy-secrets-%'
  AND r.last_event_id IS NULL;

INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
SELECT
    r.resource_uid,
    'SecretSet',
    r.created_at,
    'ReconciliationStarted',
    json_object(
        'ReconciliationStarted', json_object(
            'event_time', r.created_at,
            'uid',        r.resource_uid,
            'generation', 1
        )
    )
FROM resources r
WHERE r.resource_kind = 'SecretSet'
  AND r.resource_name LIKE 'legacy-secrets-%'
  AND r.last_event_id IS NULL;

INSERT INTO resource_events (resource_uid, resource_kind, event_time, event_type, event_payload)
SELECT
    r.resource_uid,
    'SecretSet',
    r.created_at,
    'ReconciliationSucceeded',
    '{"ReconciliationSucceeded":{"event_time":"' || r.created_at || '","uid":"' || r.resource_uid
        || '","generation":1,"success":{"stats":' || json_extract(r.status, '$.stats') || '}}}'
FROM resources r
WHERE r.resource_kind = 'SecretSet'
  AND r.resource_name LIKE 'legacy-secrets-%'
  AND r.last_event_id IS NULL;

UPDATE resources
SET last_event_id = (
    SELECT MAX(e.event_id)
    FROM resource_events e
    WHERE e.resource_uid   = resources.resource_uid
      AND e.resource_kind  = 'SecretSet'
      AND e.event_type     = 'ReconciliationSucceeded'
)
WHERE resource_kind  = 'SecretSet'
  AND resource_name  LIKE 'legacy-secrets-%'
  AND last_event_id  IS NULL;

/* ------------------------------ */

/* Drop legacy table — all data has been promoted to resources above */
DROP TABLE dataset_env_vars;

/* ------------------------------ */
