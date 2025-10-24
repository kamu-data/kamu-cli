/* ------------------------------ */

CREATE TABLE dataset_data_blocks (
    dataset_id         VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name     VARCHAR(50) NOT NULL,
    event_type         metadata_event_type NOT NULL CHECK (
        event_type = 'AddData' OR event_type = 'ExecuteTransform'
    ),
    sequence_number    BIGINT NOT NULL,
    block_hash_bin     BYTEA NOT NULL,
    block_hash_hex     TEXT GENERATED ALWAYS AS (('f1620' || encode(block_hash_bin, 'hex'))) VIRTUAL,
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

DROP TABLE dataset_key_blocks;

CREATE TABLE dataset_key_blocks (
    dataset_id         VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name     VARCHAR(50) NOT NULL,
    event_type         metadata_event_type NOT NULL CHECK (
        event_type != 'AddData' AND event_type != 'ExecuteTransform'
    ),
    sequence_number    BIGINT NOT NULL,
    block_hash_bin     BYTEA NOT NULL,
    block_hash_hex     TEXT GENERATED ALWAYS AS (('f1620' || encode(block_hash_bin, 'hex'))) VIRTUAL,
    block_payload      BYTEA NOT NULL,
    PRIMARY KEY (dataset_id, block_ref_name, sequence_number)
);

-- Optimized for fast latest-event lookup
CREATE INDEX idx_dataset_key_blocks_latest_event 
    ON dataset_key_blocks (dataset_id, block_ref_name, event_type, sequence_number DESC);

-- Optimized for block hash lookups
CREATE UNIQUE INDEX idx_dataset_key_blocks_by_hash
    ON dataset_key_blocks (dataset_id, block_hash_bin);    

/* ------------------------------ */

-- Migrate dataset_references table to use binary storage with virtual hex field
-- First, add the new binary column
ALTER TABLE dataset_references 
ADD COLUMN block_hash_bin BYTEA;

-- Convert existing textual hashes to binary format
-- The format is f1620<hex-digest>, so we strip the f1620 prefix and decode the hex
UPDATE dataset_references 
SET block_hash_bin = decode(substring(block_hash from 6), 'hex')
WHERE block_hash LIKE 'f1620%';

-- Make the binary column NOT NULL after data migration
ALTER TABLE dataset_references 
ALTER COLUMN block_hash_bin SET NOT NULL;

-- Add the virtual generated hex column
ALTER TABLE dataset_references 
ADD COLUMN block_hash_hex TEXT GENERATED ALWAYS AS (('f1620' || encode(block_hash_bin, 'hex'))) VIRTUAL;

-- Drop the old textual column
ALTER TABLE dataset_references 
DROP COLUMN block_hash;

/* ------------------------------ */
