/* ------------------------------ */

DROP INDEX idx_flows_system_scope;
DROP INDEX idx_flows_dataset_scope;
DROP INDEX idx_flows_full_binding;
DROP INDEX idx_flows_flow_status;

CREATE INDEX idx_flows_active_scope_last
  ON flows (
    json_extract(scope_data,'$'),
    flow_type,
    flow_id DESC
  )
  WHERE flow_status <> 'finished';

CREATE INDEX idx_flows_scope_last_all
  ON flows (
    json_extract(scope_data,'$'),
    flow_type,
    flow_id DESC
  );

CREATE INDEX idx_flows_ready_by_time
  ON flows (scheduled_for_activation_at, flow_id)
  WHERE scheduled_for_activation_at IS NOT NULL
    AND flow_status IN ('waiting','retrying');

CREATE INDEX idx_flows_status_last_event
  ON flows (flow_status, last_event_id DESC, flow_id DESC);

CREATE INDEX idx_flows_dataset_status_last_event
  ON flows (
    json_extract(scope_data,'$.dataset_id'),
    flow_status,
    last_event_id DESC,
    flow_id DESC
  );

CREATE INDEX idx_flow_events_finished_by_flow
  ON flow_events (flow_id, event_id)
  WHERE event_type = 'FlowEventTaskFinished';

CREATE INDEX idx_flows_scope_type
  ON flows (json_extract(scope_data,'$.type'));

CREATE INDEX idx_flows_scope_dataset
  ON flows (json_extract(scope_data,'$.dataset_id'));

CREATE INDEX idx_flows_dataset_initiator_nonsystem
  ON flows (json_extract(scope_data,'$.dataset_id'), initiator)
  WHERE initiator <> '<system>';
  
/* ------------------------------ */
