/* ------------------------------ */

-- Latest SUCCESS per flow
CREATE INDEX IF NOT EXISTS idx_flow_events_finished_with_success
  ON flow_events (event_id DESC, event_time, flow_id)
  WHERE event_type = 'FlowEventTaskFinished'
    AND json_extract(event_payload, '$.TaskFinished.task_outcome.Success') IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_flow_events_finished_with_failure
  ON flow_events (event_id DESC, event_time, flow_id)
  WHERE event_type = 'FlowEventTaskFinished'
    AND json_extract(event_payload, '$.TaskFinished.task_outcome.Failed') IS NOT NULL;

-- Recreate existing “attempt” index to INCLUDE event_time for index-only reads.
DROP INDEX IF EXISTS idx_flow_events_finished_by_flow;

CREATE INDEX idx_flow_events_finished_by_flow_desc
  ON flow_events (event_id DESC, event_time, flow_id)
  WHERE event_type = 'FlowEventTaskFinished';

/* ------------------------------ */
