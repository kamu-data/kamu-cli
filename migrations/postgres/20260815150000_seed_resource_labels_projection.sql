/* ------------------------------ */

-- Seeds the label projection from resources that already carry labels --
-- notably the `legacy-config-target-dataset` label stamped by the env-var
-- backfill, which runs earlier. Only top-level string-valued labels are
-- indexed, matching what the projection maintains at runtime.
--
-- Kept separate from the migration that creates the table so the two backends
-- stay in step.
--
-- The predicate differs from the SQLite variant by necessity: `jsonb_each`
-- keeps each value as `jsonb`, so `jsonb_typeof` is safe here, whereas SQLite's
-- `json_each` decodes the value and must be filtered on its own `type` column.
INSERT INTO resource_labels_projection (resource_id, label_key, label_value)
SELECT
    r.resource_id,
    label.key,
    label.value #>> '{}'
FROM resources r
CROSS JOIN LATERAL jsonb_each(r.labels) AS label(key, value)
WHERE jsonb_typeof(label.value) = 'string'
ON CONFLICT (resource_id, label_key) DO NOTHING;

/* ------------------------------ */
