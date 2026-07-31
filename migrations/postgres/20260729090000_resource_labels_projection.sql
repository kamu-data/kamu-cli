/* ------------------------------ */

-- Rebuildable projection over top-level string-valued `resources.labels`.
CREATE TABLE resource_labels_projection (
    resource_id             UUID NOT NULL,
    label_key               TEXT NOT NULL,
    label_value             TEXT NOT NULL,

    PRIMARY KEY (resource_id, label_key),

    CONSTRAINT fk_resource_labels_projection_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id) ON DELETE CASCADE
);

-- Covers filtered list lookups by `(label_key, label_value)`.
CREATE INDEX idx_resource_labels_projection_key_value_resource_id
    ON resource_labels_projection (label_key, label_value, resource_id);

/* ------------------------------ */
