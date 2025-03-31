/* ------------------------------ */

-- Simulate enum using CHECK constraint
CREATE TABLE dataset_key_blocks (
    dataset_id         TEXT NOT NULL,
    block_ref_name     TEXT NOT NULL,
    event_type         TEXT NOT NULL CHECK (
        event_type IN (
            'Seed',
            'SetPollingSource',
            'SetTransform',
            'SetVocab',
            'SetAttachments',
            'SetInfo',
            'SetLicense',
            'SetDataSchema',
            'AddPushSource',
            'DisablePushSource',
            'DisablePollingSource'
        )
    ),
    sequence_number    INTEGER NOT NULL,
    block_hash         TEXT NOT NULL,
    event_payload      TEXT NOT NULL,  -- Store as JSON string; SQLite has no JSONB
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),

    PRIMARY KEY (dataset_id, block_ref_name, sequence_number)
);

-- Enforce global uniqueness of block_hash
CREATE UNIQUE INDEX idx_dataset_key_blocks_block_hash
    ON dataset_key_blocks (block_hash);

-- Optimized index for latest-event lookup
CREATE INDEX idx_dataset_key_blocks_latest_event 
    ON dataset_key_blocks (dataset_id, block_ref_name, event_type, sequence_number DESC);

/* ------------------------------ */
