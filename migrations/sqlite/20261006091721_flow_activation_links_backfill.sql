/* ------------------------------ */

-- Links from the flow history recorded before the projector existed, decoded the same way
-- FlowActivationLinkProjector decodes new events. Migrations run with the server stopped,
-- so the projector then starts right after the newest event, without a replay.

INSERT INTO flow_activation_links (upstream_flow_id, downstream_flow_id, activated_at)
SELECT
    json_extract(e.cause, '$.details.source.UpstreamFlow.flow_id'),
    e.flow_id,
    e.event_time
FROM (
    SELECT
        flow_id,
        event_id,
        event_time,
        CASE event_type
            WHEN 'FlowEventInitiated'
                THEN json_extract(event_payload, '$.Initiated.activation_cause.ResourceUpdate')
            ELSE json_extract(event_payload, '$.ActivationCauseAdded.activation_cause.ResourceUpdate')
        END AS cause
    FROM flow_events
    WHERE event_type IN ('FlowEventInitiated', 'FlowEventActivationCauseAdded')
) e
WHERE
    json_extract(e.cause, '$.resource_type') = 'dev.kamu.resource.DatasetResource'
    AND json_extract(e.cause, '$.details.source.UpstreamFlow.flow_id') IS NOT NULL
    -- A cause added after the flow got a task is late: this flow does not process it
    AND NOT EXISTS (
        SELECT 1
        FROM flow_events t
        WHERE
            t.flow_id = e.flow_id
            AND t.event_type = 'FlowEventTaskScheduled'
            AND t.event_id < e.event_id
    )
ORDER BY e.event_id
ON CONFLICT (upstream_flow_id, downstream_flow_id) DO NOTHING;

INSERT INTO flow_system_projected_offsets (projector, last_event_id)
SELECT 'dev.kamu.domain.flow-system.FlowActivationLinkProjector', event_id
FROM flow_system_events
WHERE true
ORDER BY event_id DESC
LIMIT 1
ON CONFLICT (projector) DO NOTHING;

/* ------------------------------ */
