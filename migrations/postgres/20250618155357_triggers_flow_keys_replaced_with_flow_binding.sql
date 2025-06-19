/* ------------------------------ */

ALTER TABLE flow_trigger_events ADD COLUMN flow_type VARCHAR(100);
ALTER TABLE flow_trigger_events ADD COLUMN scope_data JSONB;

UPDATE flow_trigger_events
SET
    flow_type = CASE
        WHEN dataset_flow_type IS NOT NULL THEN
            CASE dataset_flow_type
                WHEN 'ingest' THEN 'dev.kamu.flow.dataset.ingest'
                WHEN 'execute_transform' THEN 'dev.kamu.flow.dataset.transform'
                WHEN 'hard_compaction' THEN 'dev.kamu.flow.dataset.compact'
                WHEN 'reset' THEN 'dev.kamu.flow.dataset.reset'
            END
        WHEN system_flow_type IS NOT NULL THEN
            CASE system_flow_type
                WHEN 'gc' THEN 'dev.kamu.flow.system.gc'
            END
    END,

    scope_data = CASE
        WHEN dataset_flow_type IS NOT NULL THEN
            jsonb_build_object('type', 'Dataset', 'dataset_id', dataset_id)
        WHEN system_flow_type IS NOT NULL THEN
            jsonb_build_object('type', 'System')
    END
WHERE flow_type IS NULL;

CREATE INDEX idx_flow_trigger_events_dataset_scope
    ON flow_trigger_events (flow_type, (scope_data->>'dataset_id'))
    WHERE scope_data->>'type' = 'Dataset';

CREATE INDEX idx_flow_trigger_events_system_scope
    ON flow_trigger_events (flow_type)
    WHERE scope_data->>'type' = 'System';

DROP INDEX IF EXISTS idx_flow_trigger_events_dataset_flow_key;
DROP INDEX IF EXISTS idx_flow_trigger_events_system_flow_key;

ALTER TABLE flow_trigger_events
    DROP COLUMN dataset_id,
    DROP COLUMN dataset_flow_type,
    DROP COLUMN system_flow_type;

ALTER TABLE flow_trigger_events
    ALTER COLUMN flow_type SET NOT NULL,
    ALTER COLUMN scope_data SET NOT NULL;

/* ------------------------------ */
