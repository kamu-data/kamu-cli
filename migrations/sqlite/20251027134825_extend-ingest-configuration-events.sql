/* ------------------------------ */
-- Update flow_configuration_events table

UPDATE flow_configuration_events
SET event_payload = json_set(
    event_payload,
    '$.Created.rule.IngestRule',
    json_patch(
        json_extract(event_payload, '$.Created.rule.IngestRule'),
        '{"fetch_next_iteration": false}'
    )
)
WHERE json_extract(event_payload, '$.Created.rule.IngestRule') IS NOT NULL
  AND json_type(json_extract(event_payload, '$.Created.rule.IngestRule')) = 'object'
  AND json_extract(event_payload, '$.Created.rule.IngestRule.fetch_next_iteration') IS NULL;

UPDATE flow_configuration_events
SET event_payload = json_set(
    event_payload,
    '$.Modified.rule.IngestRule',
    json_patch(
        json_extract(event_payload, '$.Modified.rule.IngestRule'),
        '{"fetch_next_iteration": false}'
    )
)
WHERE json_extract(event_payload, '$.Modified.rule.IngestRule') IS NOT NULL
  AND json_type(json_extract(event_payload, '$.Modified.rule.IngestRule')) = 'object'
  AND json_extract(event_payload, '$.Modified.rule.IngestRule.fetch_next_iteration') IS NULL;


/* ------------------------------ */
-- Update flow_events table

UPDATE flow_events
SET event_payload = json_set(
    event_payload,
    '$.Initiated.config_snapshot.IngestRule',
    json_patch(
        json_extract(event_payload, '$.Initiated.config_snapshot.IngestRule'),
        '{"fetch_next_iteration": false}'
    )
)
WHERE event_type = 'FlowEventInitiated'
  AND json_extract(event_payload, '$.Initiated.config_snapshot.IngestRule') IS NOT NULL
  AND json_type(json_extract(event_payload, '$.Initiated.config_snapshot.IngestRule')) = 'object'
  AND json_extract(event_payload, '$.Initiated.config_snapshot.IngestRule.fetch_next_iteration') IS NULL;

UPDATE flow_events
SET event_payload = json_set(
    event_payload,
    '$.ConfigSnapshotModified.config_snapshot.IngestRule',
    json_patch(
        json_extract(event_payload, '$.ConfigSnapshotModified.config_snapshot.IngestRule'),
        '{"fetch_next_iteration": false}'
    )
)
WHERE event_type = 'FlowConfigSnapshotModified'
  AND json_extract(event_payload, '$.ConfigSnapshotModified.config_snapshot.IngestRule') IS NOT NULL
  AND json_type(json_extract(event_payload, '$.ConfigSnapshotModified.config_snapshot.IngestRule')) = 'object'
  AND json_extract(event_payload, '$.ConfigSnapshotModified.config_snapshot.IngestRule.fetch_next_iteration') IS NULL;

/* ------------------------------ */
