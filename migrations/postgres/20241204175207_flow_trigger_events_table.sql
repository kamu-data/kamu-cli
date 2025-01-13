/* ------------------------------ */

CREATE SEQUENCE flow_trigger_event_id_seq AS BIGINT;

/* ------------------------------ */

CREATE TABLE flow_trigger_events
(
    event_id          BIGINT PRIMARY KEY DEFAULT NEXTVAL('flow_trigger_event_id_seq'),
    dataset_id        VARCHAR(100),
    dataset_flow_type dataset_flow_type,
    system_flow_type  system_flow_type,
    event_type        VARCHAR(50)       NOT NULL,
    event_time        TIMESTAMPTZ       NOT NULL,
    event_payload     JSONB             NOT NULL
);

CREATE INDEX idx_flow_trigger_events_dataset_flow_key 
   ON flow_trigger_events (dataset_id, dataset_flow_type)
   WHERE dataset_id IS NOT NULL;
   
CREATE INDEX idx_flow_trigger_events_system_flow_key
   ON flow_trigger_events (system_flow_type) 
   WHERE system_flow_type IS NOT NULL;

/* ------------------------------ */
