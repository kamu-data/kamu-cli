delete from outbox_message_consumptions where producer_name='dev.kamu.domain.flow-system.FlowConfigurationService';

TRUNCATE TABLE flow_configuration_events RESTART IDENTITY;
