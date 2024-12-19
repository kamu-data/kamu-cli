/* ------------------------------ */

UPDATE outbox_messages SET producer_name = 'dev.kamu.domain.task-system.TaskAgent'
    WHERE producer_name = 'dev.kamu.domain.task-system.TaskExecutor';
UPDATE outbox_messages SET producer_name = 'dev.kamu.domain.flow-system.FlowAgent'
    WHERE producer_name = 'dev.kamu.domain.flow-system.FlowExecutor';

UPDATE outbox_message_consumptions SET producer_name = 'dev.kamu.domain.task-system.TaskAgent'
    WHERE producer_name = 'dev.kamu.domain.task-system.TaskExecutor';
UPDATE outbox_message_consumptions SET producer_name = 'dev.kamu.domain.flow-system.FlowAgent'
    WHERE producer_name = 'dev.kamu.domain.flow-system.FlowExecutor';

UPDATE outbox_message_consumptions SET consumer_name = 'dev.kamu.domain.flow-system.FlowAgent'
    WHERE consumer_name = 'dev.kamu.domain.flow-system.FlowExecutor';

/* ------------------------------ */
