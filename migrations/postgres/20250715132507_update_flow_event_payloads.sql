BEGIN;

/* ------------------------------ */
-- Update flow_bindings in flow_events

UPDATE flow_events
SET event_payload = jsonb_set(
    event_payload || jsonb_build_object(
        'Initiated',
        (event_payload -> 'Initiated') - 'flow_key'
    ),
    '{Initiated,flow_binding}',
    COALESCE(
        CASE
            WHEN (event_payload #> '{Initiated,flow_key,Dataset}') IS NOT NULL THEN
                jsonb_build_object(
                    'flow_type', 
                    CASE (event_payload #>> '{Initiated,flow_key,Dataset,flow_type}')
                        WHEN 'Ingest' THEN 'dev.kamu.flow.dataset.ingest'
                        WHEN 'ExecuteTransform' THEN 'dev.kamu.flow.dataset.transform'
                        WHEN 'HardCompaction' THEN 'dev.kamu.flow.dataset.compact'
                        WHEN 'Reset' THEN 'dev.kamu.flow.dataset.reset'
                        ELSE 'UNKNOWN'
                    END,
                    'scope',
                    jsonb_build_object(
                        'type', 'Dataset',
                        'dataset_id', (event_payload #>> '{Initiated,flow_key,Dataset,dataset_id}')
                    )
                )
            WHEN (event_payload #> '{Initiated,flow_key,System}') IS NOT NULL THEN
                jsonb_build_object(
                    'flow_type', 
                    CASE (event_payload #>> '{Initiated,flow_key,System,flow_type}')
                        WHEN 'GC' THEN 'dev.kamu.flow.system.gc'
                        ELSE 'UNKNOWN'
                    END,
                    'scope',
                    jsonb_build_object(
                        'type', 'System'
                    )
                )
            ELSE NULL
        END,
        '{}'::jsonb
    )
)
WHERE event_payload #> '{Initiated,flow_key}' IS NOT NULL;

/* ------------------------------ */
--Modify flow_type in FlowTriggerAdded event

UPDATE flow_events
SET event_payload = jsonb_set(
    event_payload,
    ARRAY['TriggerAdded','trigger','InputDatasetFlow','flow_type'],
    to_jsonb(
      CASE event_payload #>> '{TriggerAdded,trigger,InputDatasetFlow,flow_type}'
        WHEN 'Ingest'           THEN 'dev.kamu.flow.dataset.ingest'
        WHEN 'ExecuteTransform' THEN 'dev.kamu.flow.dataset.transform'
        WHEN 'HardCompaction'   THEN 'dev.kamu.flow.dataset.compact'
        WHEN 'Reset'            THEN 'dev.kamu.flow.dataset.reset'
        ELSE 'UNKNOWN'
      END
    )
)
WHERE event_type = 'FlowEventTriggerAdded'
  AND event_payload #>> '{TriggerAdded,trigger,InputDatasetFlow,flow_type}'
      IN ('Ingest','ExecuteTransform','HardCompaction','Reset');

/* ------------------------------ */
-- Update Trigger flow result

UPDATE flow_events
SET event_payload = jsonb_set(
  event_payload,
  CASE
    WHEN event_payload ? 'Initiated'
      THEN ARRAY['Initiated','trigger','InputDatasetFlow']
    ELSE
      ARRAY['TriggerAdded','trigger','InputDatasetFlow']
  END,
  (
    (
      CASE
        WHEN event_payload ? 'Initiated'
          THEN (event_payload #> '{Initiated,trigger,InputDatasetFlow}')::jsonb
        ELSE
          (event_payload #> '{TriggerAdded,trigger,InputDatasetFlow}')::jsonb
      END
    ) - 'flow_result'
    ||
    (
      SELECT jsonb_build_object(
        'task_result',
        jsonb_build_object(
          CASE t.key
            WHEN 'DatasetCompact' THEN 'CompactionDatasetResult'
            WHEN 'DatasetUpdate'  THEN 'UpdateDatasetResult'
            ELSE t.key
          END,
          CASE t.key
            WHEN 'DatasetUpdate' THEN
              jsonb_build_object(
                'pull_result',
                CASE
                  WHEN (t.value ? 'PollingIngest') THEN t.value
                  WHEN (t.value ? 'UpToDate') THEN
                    jsonb_build_object(
                      'UpToDate',
                      jsonb_build_object(
                        'PollingIngest',
                        t.value -> 'UpToDate'
                      )
                    )
                  ELSE
                    t.value
                END
              )
            ELSE
              t.value
          END
        )
      )
      FROM jsonb_each(
        CASE
          WHEN event_payload ? 'Initiated'
            THEN (event_payload #> '{Initiated,trigger,InputDatasetFlow,flow_result}')::jsonb
          ELSE
            (event_payload #> '{TriggerAdded,trigger,InputDatasetFlow,flow_result}')::jsonb
        END
      ) AS t(key, value)
      LIMIT 1
    )
  )
)
WHERE
  event_payload #> '{Initiated,trigger,InputDatasetFlow,flow_result}' IS NOT NULL
  OR
  event_payload #> '{TriggerAdded,trigger,InputDatasetFlow,flow_result}' IS NOT NULL;

/* ------------------------------ */
--Update flow_trigger_events

UPDATE flow_trigger_events
SET event_payload =
     event_payload
     #- '{Created,flow_key}'
     #- '{Modified,flow_key}'
     #- '{DatasetRemoved,flow_key}'
     ||
     CASE
       WHEN event_payload ? 'Created' THEN
         jsonb_build_object(
           'Created',
           (
             (event_payload->'Created')
             #- '{flow_key}'
           )
           || jsonb_build_object(
                'flow_binding',
                jsonb_build_object(
                  'flow_type', flow_type,
                  'scope',
                  CASE
                    WHEN (event_payload->'Created'->'flow_key') ? 'Dataset'
                      THEN jsonb_build_object(
                             'type',       'Dataset',
                             'dataset_id', event_payload->'Created'->'flow_key'->'Dataset'->>'dataset_id'
                           )
                    WHEN (event_payload->'Created'->'flow_key') ? 'System'
                      THEN jsonb_build_object('type','System')
                  END
                )
              )
         )
       ELSE '{}'::jsonb
     END
     ||
     CASE
       WHEN event_payload ? 'Modified' THEN
         jsonb_build_object(
           'Modified',
           (
             (event_payload->'Modified')
             #- '{flow_key}'
           )
           || jsonb_build_object(
                'flow_binding',
                jsonb_build_object(
                  'flow_type', flow_type,
                  'scope',
                  CASE
                    WHEN (event_payload->'Modified'->'flow_key') ? 'Dataset'
                      THEN jsonb_build_object(
                             'type',       'Dataset',
                             'dataset_id', event_payload->'Modified'->'flow_key'->'Dataset'->>'dataset_id'
                           )
                    WHEN (event_payload->'Modified'->'flow_key') ? 'System'
                      THEN jsonb_build_object('type','System')
                  END
                )
              )
         )
       ELSE '{}'::jsonb
     END
     ||
     CASE
       WHEN event_payload ? 'DatasetRemoved' THEN
         jsonb_build_object(
           'DatasetRemoved',
           (
             (event_payload->'DatasetRemoved')
             #- '{flow_key}'
           )
           || jsonb_build_object(
                'flow_binding',
                jsonb_build_object(
                  'flow_type', flow_type,
                  'scope',
                  CASE
                    WHEN (event_payload->'DatasetRemoved'->'flow_key') ? 'Dataset'
                      THEN jsonb_build_object(
                             'type',       'Dataset',
                             'dataset_id', event_payload->'DatasetRemoved'->'flow_key'->'Dataset'->>'dataset_id'
                           )
                    WHEN (event_payload->'DatasetRemoved'->'flow_key') ? 'System'
                      THEN jsonb_build_object('type','System')
                  END
                )
              )
         )
       ELSE '{}'::jsonb
     END
WHERE event_payload ? 'Created'
   OR event_payload ? 'Modified'
   OR event_payload ? 'DatasetRemoved';

/* ------------------------------ */
-- Update flow_configuration_events

UPDATE flow_configuration_events
SET event_payload =
     event_payload
     #- '{Created,flow_key}'
     #- '{Modified,flow_key}'
     #- '{DatasetRemoved,flow_key}'
     ||
     CASE
       WHEN event_payload ? 'Created' THEN
         jsonb_build_object(
           'Created',
           (
             (event_payload->'Created')
             #- '{flow_key}'
           )
           || jsonb_build_object(
                'flow_binding',
                jsonb_build_object(
                  'flow_type', flow_type,
                  'scope',
                  CASE
                    WHEN (event_payload->'Created'->'flow_key') ? 'Dataset'
                      THEN jsonb_build_object(
                             'type',       'Dataset',
                             'dataset_id', event_payload->'Created'->'flow_key'->'Dataset'->>'dataset_id'
                           )
                    WHEN (event_payload->'Created'->'flow_key') ? 'System'
                      THEN jsonb_build_object('type','System')
                  END
                )
              )
         )
       ELSE '{}'::jsonb
     END
     ||
     CASE
       WHEN event_payload ? 'Modified' THEN
         jsonb_build_object(
           'Modified',
           (
             (event_payload->'Modified')
             #- '{flow_key}'
           )
           || jsonb_build_object(
                'flow_binding',
                jsonb_build_object(
                  'flow_type', flow_type,
                  'scope',
                  CASE
                    WHEN (event_payload->'Modified'->'flow_key') ? 'Dataset'
                      THEN jsonb_build_object(
                             'type',       'Dataset',
                             'dataset_id', event_payload->'Modified'->'flow_key'->'Dataset'->>'dataset_id'
                           )
                    WHEN (event_payload->'Modified'->'flow_key') ? 'System'
                      THEN jsonb_build_object('type','System')
                  END
                )
              )
         )
       ELSE '{}'::jsonb
     END
     ||
     CASE
       WHEN event_payload ? 'DatasetRemoved' THEN
         jsonb_build_object(
           'DatasetRemoved',
           (
             (event_payload->'DatasetRemoved')
             #- '{flow_key}'
           )
           || jsonb_build_object(
                'flow_binding',
                jsonb_build_object(
                  'flow_type', flow_type,
                  'scope',
                  CASE
                    WHEN (event_payload->'DatasetRemoved'->'flow_key') ? 'Dataset'
                      THEN jsonb_build_object(
                             'type',       'Dataset',
                             'dataset_id', event_payload->'DatasetRemoved'->'flow_key'->'Dataset'->>'dataset_id'
                           )
                    WHEN (event_payload->'DatasetRemoved'->'flow_key') ? 'System'
                      THEN jsonb_build_object('type','System')
                  END
                )
              )
         )
       ELSE '{}'::jsonb
     END
WHERE event_payload ? 'Created'
   OR event_payload ? 'Modified'
   OR event_payload ? 'DatasetRemoved';

COMMIT;
