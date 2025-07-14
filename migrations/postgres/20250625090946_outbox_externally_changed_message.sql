/* ------------------------------ */

UPDATE outbox_message_consumptions
    SET consumer_name = 'dev.kamu.domain.flow-system.FlowDispatcherIngest'
    WHERE consumer_name = 'dev.kamu.domain.flow-system.FlowAgent' AND producer_name = 'dev.kamu.adapter.http.HttpAdapter';

/* ------------------------------ */
