
/* ------------------------------ */

CREATE TYPE key_metadata_event_type AS ENUM (
    'seed',
    'set_polling_source',
    'set_transform',
    'set_vocab',
    'set_attachments',
    'set_info',
    'set_license',
    'set_data_schema',
    'add_push_source'
);


/* ------------------------------ */

CREATE TABLE dataset_key_blocks
(
    dataset_id  VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id) ON DELETE CASCADE,
    block_event_type key_metadata_event_type NOT NULL,
    block_extra_key VARCHAR(100),
    block_sequence_number BIGINT NOT NULL,
    block_hash VARCHAR(100) NOT NULL,
    block_payload JSONB NOT NULL
);

CREATE UNIQUE INDEX idx_dataset_key_blocks
    ON dataset_key_blocks (dataset_id, block_event_type, block_extra_key);

/* ------------------------------ */

