/* ------------------------------ */

-- Seeds the label projection from resources that already carry labels --
-- notably the `legacy-config-target-dataset` label stamped by the env-var
-- backfill, which runs earlier. Only top-level string-valued labels are
-- indexed, matching what the projection maintains at runtime.
--
-- Kept separate from the migration that creates the table so the two backends
-- stay in step.
--
-- Filtering uses `json_each`'s own `type` column rather than
-- `json_type(label.value)`: `json_each` already decodes each member, so
-- `value` is the bare string (`did:odf:...`), and passing that back to
-- `json_type` makes it parse a non-JSON document and fail with
-- "malformed JSON".
INSERT INTO resource_labels_projection (resource_id, label_key, label_value)
SELECT
    r.resource_id,
    label.key,
    label.value
FROM resources r, json_each(r.labels) AS label
WHERE label.type = 'text'
ON CONFLICT (resource_id, label_key) DO NOTHING;

/* ------------------------------ */
