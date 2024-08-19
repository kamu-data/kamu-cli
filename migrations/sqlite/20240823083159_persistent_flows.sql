/* ------------------------------ */

CREATE TABLE flow_ids
(
    flow_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    created_time timestamptz NOT NULL
);

/* ------------------------------ */

CREATE TABLE flows
(
    flow_id BIGINT NOT NULL PRIMARY KEY REFERENCES flow_ids(flow_id),
    dataset_id VARCHAR(100),
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
    initiator VARCHAR(100) NOT NULL,  /* No referential integrity with account_id, as it can system initiator value */
    flow_status VARCHAR(10) CHECK (
        flow_status IN (
           'waiting', 
           'running', 
           'finished'
        )
    ) NOT NULL
);

CREATE INDEX idx_flows_dataset_id ON flows (dataset_id) WHERE dataset_id IS NOT NULL;
CREATE INDEX idx_flows_system_flow_type ON flows (system_flow_type) WHERE system_flow_type IS NOT NULL;
CREATE INDEX idx_flows_flow_status ON flows(flow_status) WHERE flow_status != 'finished'; 

/* ------------------------------ */

CREATE TABLE flow_events
(
    event_id      INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    flow_id       BIGINT NOT NULL REFERENCES flows(flow_id),
    event_type    VARCHAR(50) NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE INDEX idx_flow_events_flow_id ON flow_events (flow_id);

/* ------------------------------ */
