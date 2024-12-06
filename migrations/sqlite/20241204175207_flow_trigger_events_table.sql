/* ------------------------------ */

CREATE TABLE flow_trigger_events
(
    event_id          INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    created_time      timestamptz NOT NULL,
    dataset_id        VARCHAR(100),
    dataset_flow_type VARCHAR(20) CHECK ( 
        dataset_flow_type IN (
            'ingest', 
            'execute_transform', 
            'hard_compaction', 
            'reset'
        ) 
    ),
    system_flow_type VARCHAR(10) CHECK ( 
        system_flow_type IN ('gc') 
    ),
    event_type       VARCHAR(50) NOT NULL,
    event_time       TIMESTAMPTZ NOT NULL,
    event_payload    JSONB NOT NULL
);


CREATE INDEX idx_flow_trigger_events_dataset_id_idx
     ON flow_trigger_events (dataset_id, dataset_flow_type)
     WHERE dataset_id IS NOT NULL;

CREATE INDEX idx_flow_trigger_events_system_flow_type_idx
     ON flow_trigger_events (system_flow_type)
     WHERE system_flow_type IS NOT NULL;

/* ------------------------------ */
