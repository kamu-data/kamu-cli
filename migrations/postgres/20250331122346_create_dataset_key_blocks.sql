CREATE TYPE metadata_event_type AS ENUM (
    'AddData',
    'ExecuteTransform',
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
);

CREATE TABLE dataset_key_blocks (
    dataset_id         VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_ref_name     VARCHAR(50) NOT NULL,
    event_type         metadata_event_type NOT NULL CHECK (
        event_type != 'AddData' AND event_type != 'SetTransform'
    ),
    sequence_number    BIGINT NOT NULL,
    block_hash         VARCHAR(70) NOT NULL,
    event_payload      JSONB NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset_id, block_ref_name, sequence_number)
);

-- Unique constraint assuming block_hash is globally unique (see note)
CREATE UNIQUE INDEX idx_dataset_key_blocks_block_hash
    ON dataset_key_blocks (block_hash);

-- Optimized for fast latest-event lookup
CREATE INDEX idx_dataset_key_blocks_latest_event 
    ON dataset_key_blocks (dataset_id, block_ref_name, event_type, sequence_number DESC);
