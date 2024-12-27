-- delete from outbox_message_consumptions where producer_name='dev.kamu.domain.flow-system.FlowConfigurationService';

DO $$
BEGIN
    -- Update `event_payload` to remove "paused" and "schedule_condition" for IngestRule in both 'Created' and 'Modified'
    UPDATE flow_configuration_events
    SET event_payload = 
        CASE
            -- Handle Ingest 'Created' case
            WHEN event_payload @> '{"Created": {"rule": {"IngestRule": {}}}}'::jsonb THEN
                jsonb_set(
                    event_payload - 'Created', 
                    '{Created}', 
                    jsonb_set(
                        (event_payload->'Created') - 'paused',
                        '{rule,IngestRule}',
                        (event_payload->'Created'->'rule'->'IngestRule') - 'schedule_condition'
                    )
                )
            -- Handle Ingest 'Modified' case
            WHEN event_payload @> '{"Modified": {"rule": {"IngestRule": {}}}}'::jsonb THEN
                jsonb_set(
                    event_payload - 'Modified', 
                    '{Modified}', 
                    jsonb_set(
                        (event_payload->'Modified') - 'paused',
                        '{rule,IngestRule}',
                        (event_payload->'Modified'->'rule'->'IngestRule') - 'schedule_condition'
                    )
                )
             -- Handle Compaction 'Created' case
            WHEN event_payload @> '{"Created": {"rule": {"CompactionRule": {}}}}'::jsonb THEN
                jsonb_set(
                    event_payload - 'Created', 
                    '{Created}', 
                    (event_payload->'Created') - 'paused'
                )
            -- Handle Compaction 'Modified' case
            WHEN event_payload @> '{"Modified": {"rule": {"CompactionRule": {}}}}'::jsonb THEN
                jsonb_set(
                    event_payload - 'Modified', 
                    '{Modified}', 
                    (event_payload->'Modified') - 'paused'
                )
            ELSE 
                event_payload
        END
    WHERE event_payload @> '{"Created": {"rule": {"IngestRule": {}}}}'::jsonb 
        OR event_payload @> '{"Modified": {"rule": {"IngestRule": {}}}}'::jsonb
        OR event_payload @> '{"Created": {"rule": {"CompactionRule": {}}}}'::jsonb
        OR event_payload @> '{"Modified": {"rule": {"CompactionRule": {}}}}'::jsonb;

    -- Delete rows where event_payload matches TransformRule pattern for both 'Created' and 'Modified'
    DELETE FROM flow_configuration_events
    WHERE event_payload @> '{"Created": {"rule": {"TransformRule": {}}}}'::jsonb
       OR event_payload @> '{"Modified": {"rule": {"TransformRule": {}}}}'::jsonb;
END $$;


-- Update flow_events payload
UPDATE flow_events
SET event_payload = jsonb_set(
    event_payload - 'Initiated',
    '{Initiated}',
    jsonb_set(
        event_payload->'Initiated',
        '{config_snapshot}',
        jsonb_set(
            event_payload->'Initiated'->'config_snapshot',
            '{IngestRule}',
            (event_payload->'Initiated'->'config_snapshot'->'Ingest') - 'schedule_condition'
        ) - 'Ingest'
    )
)
WHERE event_payload @> '{"Initiated": {"config_snapshot": {"Ingest": {"schedule_condition": {}}}}}'::jsonb;
