/*
 * Queries may ask for deleted resources: listings of them run newest deletion
 * first, the summary counts them, and label selectors and names find them.
 */

/* In the order of the listing; queries must repeat the expression and the predicate */
CREATE INDEX idx_resources_deletion_time
    ON resources (
        account_id,
        resource_schema,
        (COALESCE(deleted_at, deletion_requested_at)) DESC,
        updated_at DESC,
        resource_id DESC
    )
    WHERE deletion_requested_at IS NOT NULL;

DROP INDEX idx_resources_summary_projection;

CREATE INDEX idx_resources_summary_projection
    ON resources (
        account_id,
        resource_schema,
        ((status ->> 'phase'))
    );

/* Lookups by name that the partial unique index on live names does not cover; queries must repeat the expression */
CREATE INDEX idx_resources_account_schema_name_any_visibility
    ON resources (account_id, resource_schema, LOWER(resource_name));

/* Completing a deletion used to remove the labels of the resource from the projection */
INSERT INTO resource_labels_projection (resource_id, label_key, label_value)
SELECT
    r.resource_id,
    label.key,
    label.value #>> '{}'
FROM resources r
CROSS JOIN LATERAL jsonb_each(r.labels) AS label(key, value)
WHERE r.deleted_at IS NOT NULL
  AND jsonb_typeof(label.value) = 'string'
ON CONFLICT (resource_id, label_key) DO NOTHING;
