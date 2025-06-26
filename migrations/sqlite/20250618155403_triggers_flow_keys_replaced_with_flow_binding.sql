/* ------------------------------ */

-- Drop old table completely
DROP TABLE IF EXISTS flow_trigger_events;

-- Create new replacing table
CREATE TABLE flow_trigger_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    flow_type VARCHAR(100) NOT NULL,
    scope_data JSONB NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_time TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

-- Create dataset scope index
CREATE INDEX idx_flow_trigger_events_dataset_scope
    ON flow_trigger_events (flow_type, json_extract(scope_data, '$.dataset_id'))
    WHERE json_extract(scope_data, '$.type') = 'Dataset';

-- Create system scope index
CREATE INDEX idx_flow_trigger_events_system_scope
    ON flow_trigger_events (flow_type)
    WHERE json_extract(scope_data, '$.type') = 'System';

/* ------------------------------ */    