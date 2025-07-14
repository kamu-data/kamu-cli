/* ------------------------------ */

ALTER TABLE flows ADD COLUMN flow_type VARCHAR(100);
ALTER TABLE flows ADD COLUMN scope_data JSONB;

UPDATE flows
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

CREATE INDEX idx_flows_dataset_scope
    ON flows (flow_type, (scope_data->>'dataset_id'))
    WHERE scope_data->>'type' = 'Dataset';

CREATE INDEX idx_flows_system_scope
    ON flows (flow_type)
    WHERE scope_data->>'type' = 'System';

CREATE INDEX idx_flows_full_binding
    ON flows (flow_type, scope_data);    

DROP INDEX IF EXISTS idx_flows_dataset_id;
DROP INDEX IF EXISTS idx_flows_system_flow_type;

ALTER TABLE flows
    DROP COLUMN dataset_id,
    DROP COLUMN dataset_flow_type,
    DROP COLUMN system_flow_type;

ALTER TABLE flows
    ALTER COLUMN flow_type SET NOT NULL,
    ALTER COLUMN scope_data SET NOT NULL;

/* ------------------------------ */

UPDATE flow_events
SET event_payload = jsonb_set(
    -- Remove old flow_key first:
    (event_payload - 'flow_key'),
    -- Insert new flow_binding key:
    '{flow_binding}',
    -- Build flow_binding object dynamically:
    (
        CASE
            -- Dataset scope
            WHEN (event_payload #> '{flow_key,Dataset}') IS NOT NULL THEN
                jsonb_build_object(
                    'flow_type', 
                    CASE (event_payload #>> '{flow_key,Dataset,flow_type}')
                        WHEN 'Ingest' THEN 'dev.kamu.flow.dataset.ingest'
                        WHEN 'ExecuteTransform' THEN 'dev.kamu.flow.dataset.transform'
                        WHEN 'HardCompaction' THEN 'dev.kamu.flow.dataset.compact'
                        WHEN 'Reset' THEN 'dev.kamu.flow.dataset.reset'
                        ELSE 'UNKNOWN'
                    END,
                    'scope',
                    jsonb_build_object(
                        'type', 'Dataset',
                        'dataset_id', (event_payload #>> '{flow_key,Dataset,dataset_id}')
                    )
                )
            -- System scope
            WHEN (event_payload #> '{flow_key,System}') IS NOT NULL THEN
                jsonb_build_object(
                    'flow_type', 
                    CASE (event_payload #>> '{flow_key,System,flow_type}')
                        WHEN 'GC' THEN 'dev.kamu.flow.system.gc'
                        ELSE 'UNKNOWN'
                    END,
                    'scope',
                    jsonb_build_object(
                        'type', 'System'
                    )
                )
            ELSE NULL
        END
    )
)
WHERE event_payload ? 'flow_key';

/* ------------------------------ */

UPDATE flow_events
SET event_payload = jsonb_set(
    event_payload,
    '{Initiated,trigger,InputDatasetFlow,flow_type}',
    (
        CASE (event_payload #>> '{Initiated,trigger,InputDatasetFlow,flow_type}')
            WHEN 'Ingest' THEN '"dev.kamu.flow.dataset.ingest"'::jsonb
            WHEN 'ExecuteTransform' THEN '"dev.kamu.flow.dataset.transform"'::jsonb
            WHEN 'HardCompaction' THEN '"dev.kamu.flow.dataset.compact"'::jsonb
            WHEN 'Reset' THEN '"dev.kamu.flow.dataset.reset"'::jsonb
            ELSE '"UNKNOWN"'::jsonb
        END
    )
)
WHERE event_payload ? 'Initiated'
  AND event_payload #> '{Initiated,trigger,InputDatasetFlow}' IS NOT NULL;

/* ------------------------------ */

DROP TYPE IF EXISTS dataset_flow_type;
DROP TYPE IF EXISTS system_flow_type;

/* ------------------------------ */
