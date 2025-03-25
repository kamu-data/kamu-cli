/* ------------------------------ */

ALTER TABLE dataset_entries
    ADD COLUMN kind VARCHAR(15) NOT NULL 
        DEFAULT 'root' 
        CHECK (kind IN ('root', 'derivative'));

UPDATE dataset_entries
    SET kind = 'derivative'
    WHERE dataset_id IN (
        SELECT downstream_dataset_id FROM dataset_dependencies
    );

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

INSERT INTO outbox_message_consumptions (consumer_name, producer_name, last_consumed_message_id)
    VALUES (
        'dev.kamu.domain.datasets.DatasetStatisticsUpdateHandler', 
        'dev.kamu.domain.datasets.DatasetService', 
        (
            SELECT last_consumed_message_id 
                FROM outbox_message_consumptions
                WHERE producer_name = 'dev.kamu.domain.datasets.DatasetService'
                LIMIT 1
        )
    )

/* ------------------------------ */

