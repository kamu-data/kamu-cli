/* ------------------------------ */

UPDATE outbox_messages SET producer_name = 'dev.kamu.domain.datasets.DatasetService'
    WHERE producer_name = 'dev.kamu.domain.core.services.DatasetService';

DELETE FROM outbox_messages WHERE producer_name = 'dev.kamu.domain.datasets.DatasetService' AND 
    EXISTS (
        SELECT 1
        FROM json_each(content_json) AS kv
            WHERE kv.key IN ('DependenciesUpdated', 'Removed') 
            AND kv.value IS NOT NULL
    );

UPDATE outbox_message_consumptions SET producer_name = 'dev.kamu.domain.datasets.DatasetService'
    WHERE producer_name = 'dev.kamu.domain.core.services.DatasetService';

DELETE FROM outbox_message_consumptions WHERE consumer_name = 'dev.kamu.domain.datasets.DependencyGraphService';

/* ------------------------------ */
