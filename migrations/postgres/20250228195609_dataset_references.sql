/* ------------------------------ */

CREATE TABLE dataset_references (
    dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name VARCHAR(50) NOT NULL,
    block_hash VARCHAR(70) NOT NULL
);

CREATE UNIQUE INDEX idx_dataset_references
    ON dataset_references (dataset_id, block_ref_name);

/* ------------------------------ */
