/* ------------------------------ */

-- Simulate enum using CHECK constraint
CREATE TABLE dataset_key_blocks (
    dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name VARCHAR(50) NOT NULL,
    event_type VARCHAR(30) NOT NULL CHECK (
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
    sequence_number INTEGER NOT NULL,
    block_hash VARCHAR(70) NOT NULL,
    event_payload TEXT NOT NULL,  -- Store as JSON string; SQLite has no JSONB
    created_at TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (dataset_id, block_ref_name, sequence_number)
);

-- Optimized index for latest-event lookup
CREATE INDEX idx_dataset_key_blocks_latest_event 
    ON dataset_key_blocks (dataset_id, block_ref_name, event_type, sequence_number DESC);

/* ------------------------------ */
