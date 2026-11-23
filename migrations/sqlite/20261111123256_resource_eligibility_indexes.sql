/*
 * Partial indexes for the eligibility predicates the resource controller
 * runtime queries per schema: not reconciled at the current generation, and
 * deletion requested but not completed.
 */

/*
 * SQLite cannot use a partial index whose condition calls a function, so the
 * reconciled generation is exposed as a virtual generated column: computed
 * from the status on read, never stored or written by the application.
 */
ALTER TABLE resources ADD COLUMN reconciled_generation INTEGER
    GENERATED ALWAYS AS (COALESCE(json_extract(status, '$.reconciledGeneration'), 0)) VIRTUAL;

CREATE INDEX idx_resources_not_reconciled
    ON resources (resource_schema, resource_id)
    WHERE deletion_requested_at IS NULL AND reconciled_generation < generation;

CREATE INDEX idx_resources_deletion_pending
    ON resources (resource_schema, resource_id)
    WHERE deletion_requested_at IS NOT NULL AND deleted_at IS NULL;
