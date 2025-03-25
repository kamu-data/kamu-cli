/* ------------------------------ */

CREATE TYPE dataset_kind AS ENUM ('root', 'derivative');

/* ------------------------------ */

ALTER TABLE dataset_entries
   ADD COLUMN kind dataset_kind;

UPDATE dataset_entries
SET kind = CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM dataset_dependencies d
        WHERE d.downstream_dataset_id = dataset_entries.dataset_id
    ) THEN 'root'::dataset_kind
    ELSE 'derivative'::dataset_kind
END;

ALTER TABLE dataset_entries
    ALTER COLUMN kind SET NOT NULL;

/* ------------------------------ */

CREATE TABLE dataset_statistics (
    dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name VARCHAR(50) NOT NULL,
    last_pulled TIMESTAMPTZ DEFAULT NULL,
    num_records BIGINT NOT NULL,
    data_size BIGINT NOT NULL,
    checkpoints_size BIGINT NOT NULL
);

CREATE UNIQUE INDEX idx_dataset_statistics
    ON dataset_statistics (dataset_id, block_ref_name);

/* ------------------------------ */
