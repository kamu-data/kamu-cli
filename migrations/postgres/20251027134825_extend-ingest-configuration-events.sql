/* ------------------------------ */
-- Update flow_configuration_events table

UPDATE public.flow_configuration_events
SET event_payload = jsonb_set(
    event_payload,
    '{Created,rule,IngestRule}',
    (
        event_payload #> '{Created,rule,IngestRule}'
    ) || '{"fetch_next_iteration": false}'::jsonb
)
WHERE
    event_payload ? 'Created'
    AND (event_payload #> '{Created,rule,IngestRule}') IS NOT NULL
    AND jsonb_typeof(event_payload #> '{Created,rule,IngestRule}') = 'object'
    AND NOT (event_payload #> '{Created,rule,IngestRule}') ? 'fetch_next_iteration';

UPDATE public.flow_configuration_events
SET event_payload = jsonb_set(
    event_payload,
    '{Modified,rule,IngestRule}',
    (
        event_payload #> '{Modified,rule,IngestRule}'
    ) || '{"fetch_next_iteration": false}'::jsonb
)
WHERE
    event_payload ? 'Modified'
    AND (event_payload #> '{Modified,rule,IngestRule}') IS NOT NULL
    AND jsonb_typeof(event_payload #> '{Modified,rule,IngestRule}') = 'object'
    AND NOT (event_payload #> '{Modified,rule,IngestRule}') ? 'fetch_next_iteration';

/* ------------------------------ */
-- Update flow_events table

UPDATE public.flow_events
SET event_payload = jsonb_set(
    event_payload,
    '{Initiated,config_snapshot,IngestRule}',
    (
        event_payload #> '{Initiated,config_snapshot,IngestRule}'
    ) || '{"fetch_next_iteration": false}'::jsonb
)
WHERE
    event_type = 'FlowEventInitiated'
    AND event_payload ? 'Initiated'
    AND (event_payload #> '{Initiated,config_snapshot,IngestRule}') IS NOT NULL
    AND jsonb_typeof(event_payload #> '{Initiated,config_snapshot,IngestRule}') = 'object'
    AND NOT (event_payload #> '{Initiated,config_snapshot,IngestRule}') ? 'fetch_next_iteration';

UPDATE public.flow_events
SET event_payload = jsonb_set(
    event_payload,
    '{ConfigSnapshotModified,config_snapshot,IngestRule}',
    (
        event_payload #> '{ConfigSnapshotModified,config_snapshot,IngestRule}'
    ) || '{"fetch_next_iteration": false}'::jsonb
)
WHERE
    event_type = 'FlowConfigSnapshotModified'
    AND event_payload ? 'ConfigSnapshotModified'
    AND (event_payload #> '{ConfigSnapshotModified,config_snapshot,IngestRule}') IS NOT NULL
    AND jsonb_typeof(event_payload #> '{ConfigSnapshotModified,config_snapshot,IngestRule}') = 'object'
    AND NOT (event_payload #> '{ConfigSnapshotModified,config_snapshot,IngestRule}') ? 'fetch_next_iteration';

/* ------------------------------ */

UPDATE public.flow_events
SET event_payload = jsonb_set(
    event_payload,
    '{TaskFinished,task_outcome,Success,UpdateDatasetResult,pull_result,Updated}',
    (
        event_payload #> '{TaskFinished,task_outcome,Success,UpdateDatasetResult,pull_result,Updated}'
    ) || '{"has_more": false}'::jsonb
)
WHERE
    event_type = 'FlowEventTaskFinished'
    AND event_payload ? 'TaskFinished'
    AND (event_payload #> '{TaskFinished,task_outcome,Success,UpdateDatasetResult,pull_result,Updated}') IS NOT NULL
    AND jsonb_typeof(event_payload #> '{TaskFinished,task_outcome,Success,UpdateDatasetResult,pull_result,Updated}') = 'object'
    AND NOT (event_payload #> '{TaskFinished,task_outcome,Success,UpdateDatasetResult,pull_result,Updated}') ? 'has_more';

/* ------------------------------ */
