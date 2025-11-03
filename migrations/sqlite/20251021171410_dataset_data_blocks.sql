/* ------------------------------ */

-- Simulate enum using CHECK constraint
CREATE TABLE dataset_data_blocks (
    dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name VARCHAR(50) NOT NULL,
    event_type VARCHAR(30) NOT NULL CHECK (
        event_type IN (
            'AddData',
            'ExecuteTransform'
        )
    ),
    sequence_number INTEGER NOT NULL,
    block_hash_bin BLOB NOT NULL,
    block_hash_hex TEXT GENERATED ALWAYS AS ('f1620' || hex(block_hash_bin)) VIRTUAL NOT NULL,
    block_payload BLOB NOT NULL,
    PRIMARY KEY (dataset_id, block_ref_name, sequence_number)
);

-- Optimized index for latest-event lookup
CREATE INDEX idx_dataset_data_blocks_latest_event 
    ON dataset_data_blocks (dataset_id, block_ref_name, event_type, sequence_number DESC);

-- Optimized for block hash lookups
CREATE UNIQUE INDEX idx_dataset_data_blocks_by_hash
    ON dataset_data_blocks (dataset_id, block_hash_bin);

/* ------------------------------ */

DROP TABLE dataset_key_blocks;

CREATE TABLE dataset_key_blocks (
    dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name VARCHAR(50) NOT NULL,
    event_type VARCHAR(30) NOT NULL CHECK (
        event_type NOT IN (
            'AddData',
            'ExecuteTransform'
        )
    ),
    sequence_number INTEGER NOT NULL,
    block_hash_bin BLOB NOT NULL,
    block_hash_hex TEXT GENERATED ALWAYS AS ('f1620' || hex(block_hash_bin)) VIRTUAL NOT NULL,
    block_payload BLOB NOT NULL,
    PRIMARY KEY (dataset_id, block_ref_name, sequence_number)
);

-- Optimized index for latest-event lookup
CREATE INDEX idx_dataset_key_blocks_latest_event 
    ON dataset_key_blocks (dataset_id, block_ref_name, event_type, sequence_number DESC);

-- Optimized for block hash lookups
CREATE UNIQUE INDEX idx_dataset_key_blocks_by_hash
    ON dataset_key_blocks (dataset_id, block_hash_bin);

/* ------------------------------ */

-- Migrate dataset_references table to use binary storage with virtual hex field
-- SQLite doesn't support adding NOT NULL columns directly, so we need to recreate the table

-- Create new table with binary storage
CREATE TABLE dataset_references_new (
    dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name VARCHAR(50) NOT NULL,
    block_hash_bin BLOB NOT NULL,
    block_hash_hex TEXT GENERATED ALWAYS AS ('f1620' || hex(block_hash_bin)) VIRTUAL NOT NULL
);

-- Copy data from old table, converting hex format to binary
-- The format is f1620<hex-digest>, so we strip the f1620 prefix and convert hex to binary
INSERT INTO dataset_references_new (dataset_id, block_ref_name, block_hash_bin)
    SELECT 
        dataset_id, 
        block_ref_name, 
        unhex(substr(block_hash, 6))
    FROM dataset_references 
    WHERE block_hash LIKE 'f1620%';

-- Drop old table and rename new one
DROP TABLE dataset_references;
ALTER TABLE dataset_references_new RENAME TO dataset_references;

-- Recreate the unique index
CREATE UNIQUE INDEX idx_dataset_references
    ON dataset_references (dataset_id, block_ref_name);

/* ------------------------------ */
