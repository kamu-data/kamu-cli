/* ------------------------------ */

-- Latest SUCCESS per flow (index-only friendly)
CREATE INDEX idx_flow_events_finished_with_success_desc
  ON flow_events (event_id DESC)
  INCLUDE (event_time, flow_id)
  WHERE event_type = 'FlowEventTaskFinished'
    AND (event_payload #> '{TaskFinished,task_outcome,Success}') IS NOT NULL;

-- Latest FAILURE per flow (index-only friendly)
CREATE INDEX idx_flow_events_finished_with_failure_desc
  ON flow_events (event_id DESC)
  INCLUDE (event_time, flow_id)
  WHERE event_type = 'FlowEventTaskFinished'
    AND (event_payload #> '{TaskFinished,task_outcome,Failed}') IS NOT NULL;

-- Recreate existing “attempt” index to INCLUDE event_time for index-only reads.
DROP INDEX IF EXISTS idx_flow_events_finished_by_flow_desc;

CREATE INDEX idx_flow_events_finished_by_flow_desc
   ON flow_events (flow_id, event_id DESC)
   INCLUDE (event_time, flow_id)
   WHERE event_type = 'FlowEventTaskFinished';

/* ------------------------------ */
