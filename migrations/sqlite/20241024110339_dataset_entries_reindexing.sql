-- Inserting if there is no row.
INSERT INTO outbox_message_consumptions (consumer_name, producer_name, last_consumed_message_id)
SELECT 'dev.kamu.domain.datasets.DatasetEntryService', 'dev.kamu.domain.core.services.DatasetService', 0
WHERE NOT EXISTS (SELECT *
                  FROM outbox_message_consumptions
                  WHERE consumer_name = 'dev.kamu.domain.datasets.DatasetEntryService');

-- Skip previous `DatasetLifecycleMessage`'s because the structure has changed.
-- This does not cause problems, because `DatasetEntryService` will fill with rows based on current datasets.
UPDATE outbox_message_consumptions
SET last_consumed_message_id = (SELECT last_consumed_message_id
                                FROM outbox_message_consumptions
                                WHERE producer_name = 'dev.kamu.domain.core.services.DatasetService'
                                LIMIT 1)
WHERE consumer_name = 'dev.kamu.domain.datasets.DatasetEntryService';

-- Start re-indexing.
DELETE
FROM dataset_entries;
