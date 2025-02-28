/* ------------------------------ */

INSERT INTO outbox_message_consumptions (consumer_name, producer_name, last_consumed_message_id)
    VALUES (
        'dev.kamu.domain.datasets.DatasetAliasUpdateHandler', 
        'dev.kamu.domain.datasets.DatasetService', 
        (
            SELECT last_consumed_message_id 
                FROM outbox_message_consumptions
                WHERE producer_name = 'dev.kamu.domain.datasets.DatasetService'
                LIMIT 1
        )
    )

/* ------------------------------ */

