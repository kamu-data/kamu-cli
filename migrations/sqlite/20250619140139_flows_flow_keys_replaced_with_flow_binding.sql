/* ------------------------------ */

PRAGMA foreign_keys = OFF;

-- Step 1: Clean content
DELETE FROM flow_events;
DELETE FROM flows;

-- Step 2: Drop legacy indexes

DROP INDEX IF EXISTS idx_flows_dataset_id;
DROP INDEX IF EXISTS idx_flows_system_flow_type;

-- Step 3: Drop obsolete columns from flows table

ALTER TABLE flows DROP COLUMN dataset_id;
ALTER TABLE flows DROP COLUMN dataset_flow_type;
ALTER TABLE flows DROP COLUMN system_flow_type;

-- Step 4: Add new columns for FlowBinding model

ALTER TABLE flows ADD COLUMN flow_type TEXT NOT NULL;
ALTER TABLE flows ADD COLUMN scope_data JSONB NOT NULL;

-- Step 5: Create new FlowBinding-compatible indexes

CREATE INDEX idx_flows_dataset_scope
    ON flows (flow_type, json_extract(scope_data, '$.dataset_id'))
    WHERE json_extract(scope_data, '$.type') = 'Dataset';

CREATE INDEX idx_flows_system_scope
    ON flows (flow_type)
    WHERE json_extract(scope_data, '$.type') = 'System';

CREATE INDEX idx_flows_full_binding
    ON flows (flow_type, scope_data);        

PRAGMA foreign_keys = ON;

/* ------------------------------ */
