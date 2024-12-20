delete from outbox_message_consumptions where producer_name='dev.kamu.domain.flow-system.FlowConfigurationService';

TRUNCATE TABLE flow_configuration_events RESTART IDENTITY;
TRUNCATE TABLE flow_events CASCADE;
ALTER SEQUENCE flow_event_id_seq RESTART WITH 1;
ALTER SEQUENCE flow_id_seq RESTART WITH 1;
