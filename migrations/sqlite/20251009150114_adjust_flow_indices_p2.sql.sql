/* ------------------------------ */

CREATE INDEX idx_fte_latest_per_scope
  ON flow_trigger_events (flow_type, scope_data, event_time DESC);

DROP INDEX idx_fte_dataset_scope;
DROP INDEX idx_fte_system_scope;

/* ------------------------------ */

CREATE INDEX idx_fce_latest_per_scope
  ON flow_configuration_events (flow_type, scope_data, event_time DESC);

DROP INDEX idx_fce_dataset_scope;
DROP INDEX idx_fce_system_scope;

/* ------------------------------ */

DROP INDEX idx_fe_flow_id;

CREATE INDEX idx_fe_flow_id_event_id
  ON flow_events (flow_id, event_id);

/* ------------------------------ */

DROP INDEX idx_flows_scope_last_all;
DROP INDEX idx_flows_dataset_initiator_nonsystem;

CREATE INDEX idx_flows_initiator_nonsystem ON flows (initiator) WHERE initiator <> '<system>';

CREATE INDEX idx_flows_scope_subscription_id ON flows (json_extract(scope_data,'$.subscription_id'));
  
/* ------------------------------ */
