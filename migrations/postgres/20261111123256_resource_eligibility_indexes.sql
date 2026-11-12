/*
 * Partial indexes for the eligibility predicates the resource controller
 * runtime queries per schema: not reconciled at the current generation, and
 * deletion requested but not completed.
 */

/* Queries must repeat the predicate verbatim for the planner to use the index */
CREATE INDEX idx_resources_not_reconciled
    ON resources (resource_schema, resource_id)
    WHERE deletion_requested_at IS NULL
      AND COALESCE((status ->> 'reconciledGeneration')::BIGINT, 0) < generation;

CREATE INDEX idx_resources_deletion_pending
    ON resources (resource_schema, resource_id)
    WHERE deletion_requested_at IS NOT NULL AND deleted_at IS NULL;
