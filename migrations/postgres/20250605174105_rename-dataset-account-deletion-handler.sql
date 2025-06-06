
/* ------------------------------ */

UPDATE outbox_message_consumptions SET consumer_name = 'dev.kamu.domain.datasets.DatasetsLifecycleHandler'
    WHERE consumer_name = 'dev.kamu.domain.datasets.DatasetsDeletionHandler';

/* ------------------------------ */
