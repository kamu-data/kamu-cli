/* ------------------------------ */

CREATE TABLE dataset_dependencies
(
    upstream_dataset_id   VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id),
    downstream_dataset_id VARCHAR(100) NOT NULL REFERENCES dataset_entries(dataset_id)
);

CREATE UNIQUE INDEX idx_dataset_dependencies
    ON dataset_dependencies (upstream_dataset_id, downstream_dataset_id);

CREATE INDEX idx_dataset_dependencies_upstream_dataset_id
    ON dataset_dependencies(upstream_dataset_id);

CREATE INDEX idx_dataset_dependencies_downstream_dataset_id
    ON dataset_dependencies (downstream_dataset_id);

/* ------------------------------ */

-- Inserting if there is no row.
INSERT INTO outbox_message_consumptions (consumer_name, producer_name, last_consumed_message_id)
SELECT 'dev.kamu.domain.datasets.DependencyGraphService', 'dev.kamu.domain.core.services.DatasetService', 0
WHERE NOT EXISTS (SELECT *
                  FROM outbox_message_consumptions
                  WHERE consumer_name = 'dev.kamu.domain.datasets.DependencyGraphService');

-- Skip previous `DatasetLifecycleMessage`'s
-- This does not cause problems, because the indexer will fill with rows based on current datasets dependencies.
UPDATE outbox_message_consumptions
SET last_consumed_message_id = (SELECT last_consumed_message_id
                                FROM outbox_message_consumptions
                                WHERE producer_name = 'dev.kamu.domain.core.services.DatasetService'
                                LIMIT 1)
WHERE consumer_name = 'dev.kamu.domain.datasets.DependencyGraphService';


/* ------------------------------ */
