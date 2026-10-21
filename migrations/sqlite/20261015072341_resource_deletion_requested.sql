/*
 * Deletion of a resource has two steps: `deletion_requested_at` is set when
 * deletion is requested, `deleted_at` once the controllers cleaned up.
 *
 * Resources deleted before that went through both steps at once, and their
 * events replay so: the request takes the time of the deletion, the phase is
 * `Deleted`, and the deletion counts as one more status update.
 */

ALTER TABLE resources ADD COLUMN deletion_requested_at TIMESTAMPTZ NULL;

UPDATE resources
SET
    deletion_requested_at = deleted_at,
    status_generation = status_generation + 1,
    status = json_set(status, '$.phase', 'Deleted')
WHERE deleted_at IS NOT NULL;

-- A resource being deleted gives up its name, so a new resource can take it
DROP INDEX idx_resources_account_schema_name_ci;

CREATE UNIQUE INDEX idx_resources_account_schema_name_ci
    ON resources (account_id, resource_schema, resource_name)
    WHERE deletion_requested_at IS NULL;
