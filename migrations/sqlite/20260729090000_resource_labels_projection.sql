/* ------------------------------ */

-- Projection over `resources.labels`: one row per top-level string-valued
-- label, maintained transactionally alongside the snapshot write. Contains
-- no independent state — in principle re-derivable from `resources.labels`
-- alone (no event replay needed) — but no rebuild tooling exists yet.
CREATE TABLE resource_labels_projection (
    resource_id             CHAR(36) NOT NULL,
    label_key               TEXT NOT NULL,
    label_value             TEXT NOT NULL,

    PRIMARY KEY (resource_id, label_key),

    CONSTRAINT fk_resource_labels_projection_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id) ON DELETE CASCADE
);

-- Covers the (label_key, label_value) -> resource_id lookup used by filtered
-- `list` queries. This duplicates the PK columns of every row (the table only
-- has three narrow columns to begin with, so the extra footprint is small),
-- but including resource_id makes the index self-sufficient for that lookup
-- instead of requiring a table/PK fetch per match.
CREATE INDEX idx_resource_labels_projection_key_value_resource_id
    ON resource_labels_projection (label_key, label_value, resource_id);

/* ------------------------------ */
