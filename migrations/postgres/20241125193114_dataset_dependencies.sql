/* ------------------------------ */

CREATE TABLE dataset_dependencies
(
    -- Note: no foreign keys here, as external orphans are possible in the graph
    upstream_dataset_id   VARCHAR(100) NOT NULL,
    downstream_dataset_id VARCHAR(100) NOT NULL
);

CREATE UNIQUE INDEX idx_dataset_dependencies
    ON dataset_dependencies (upstream_dataset_id, downstream_dataset_id);

CREATE INDEX idx_dataset_dependencies_upstream_dataset_id
    ON dataset_dependencies(upstream_dataset_id);

CREATE INDEX idx_dataset_dependencies_downstream_dataset_id
    ON dataset_dependencies (downstream_dataset_id);

/* ------------------------------ */
