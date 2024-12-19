delete from outbox_message_consumptions where producer_name = "dev.kamu.domain.flow-system.FlowConfigurationService";

DELETE FROM flow_configuration_events;
