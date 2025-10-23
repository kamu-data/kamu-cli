/* ------------------------------ */

CREATE TABLE dataset_data_blocks (
    dataset_id         VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name     VARCHAR(50) NOT NULL,
    event_type         metadata_event_type NOT NULL CHECK (
        event_type = 'AddData' OR event_type = 'ExecuteTransform'
    ),
    sequence_number    BIGINT NOT NULL,
    block_hash_bin     BYTEA NOT NULL,
    block_hash_hex     TEXT GENERATED ALWAYS AS (encode(block_hash_bin, 'hex')) VIRTUAL,
    block_payload      BYTEA NOT NULL,
    PRIMARY KEY (dataset_id, block_ref_name, sequence_number)
);

-- Optimized for fast latest-event lookup
CREATE INDEX idx_dataset_data_blocks_latest_event 
    ON dataset_data_blocks (dataset_id, block_ref_name, event_type, sequence_number DESC);

-- Optimized for block hash lookups
CREATE UNIQUE INDEX idx_dataset_data_blocks_by_hash
    ON dataset_data_blocks (dataset_id, block_hash_bin);    

/* ------------------------------ */
