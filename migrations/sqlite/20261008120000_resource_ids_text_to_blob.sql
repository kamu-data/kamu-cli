/* ------------------------------ */

-- `20260815120000_accounts_add_resource_id` and
-- `20260815130000_backfill_env_var_resources` generated UUIDs as 36-char text,
-- but the repositories bind and decode `uuid::Uuid`, which `sqlx` stores in
-- SQLite as a 16-byte blob. Text ids fail to decode and never match a lookup,
-- so every pre-existing account broke the CLI startup.
--
-- Convert those ids to blobs. Only rows created by those migrations can hold
-- text ids: rows written from Rust are blobs already.

-- Children are updated before their parent `resources` rows; the foreign keys
-- are checked at commit.
PRAGMA defer_foreign_keys = ON;

/* ------------------------------ */

-- Legacy env-var resources: VariableSet / SecretSet resources labelled with
-- `LegacyConfigTargetDataset` by the backfill.
CREATE TEMP TABLE legacy_env_var_resource_ids AS
SELECT resource_id
FROM resources
WHERE typeof(resource_id) = 'text'
  AND resource_schema IN (
      'https://opendatafabric.org/schemas/config/v1alpha1/VariableSet',
      'https://opendatafabric.org/schemas/config/v1alpha1/SecretSet'
  )
  AND json_extract(
      labels,
      '$."https://kamu.dev/schemas/config/v1alpha1/labels/LegacyConfigTargetDataset"'
  ) IS NOT NULL;

UPDATE config_variable_set_entries
SET resource_id = unhex(resource_id, '-'),
    entry_id    = unhex(entry_id, '-')
WHERE resource_id IN (SELECT resource_id FROM legacy_env_var_resource_ids);

UPDATE config_secret_set_entries
SET resource_id = unhex(resource_id, '-'),
    entry_id    = unhex(entry_id, '-')
WHERE resource_id IN (SELECT resource_id FROM legacy_env_var_resource_ids);

UPDATE resource_labels_projection
SET resource_id = unhex(resource_id, '-')
WHERE resource_id IN (SELECT resource_id FROM legacy_env_var_resource_ids)
  AND label_key = 'https://kamu.dev/schemas/config/v1alpha1/labels/LegacyConfigTargetDataset';

UPDATE resource_events
SET resource_id = unhex(resource_id, '-')
WHERE resource_id IN (SELECT resource_id FROM legacy_env_var_resource_ids);

UPDATE resources
SET resource_id = unhex(resource_id, '-')
WHERE resource_id IN (SELECT resource_id FROM legacy_env_var_resource_ids);

DROP TABLE legacy_env_var_resource_ids;

/* ------------------------------ */

UPDATE accounts
SET resource_id = unhex(resource_id, '-')
WHERE typeof(resource_id) = 'text';

/* ------------------------------ */
