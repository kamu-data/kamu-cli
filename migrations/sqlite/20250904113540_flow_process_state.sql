/* ------------------------------ */

CREATE TABLE flow_process_states (
    scope_data      JSON NOT NULL,
    flow_type       TEXT NOT NULL,

    user_intent   TEXT NOT NULL DEFAULT 'undefined'
        CHECK (user_intent IN ('undefined', 'enabled', 'paused')),

    stop_policy_kind TEXT NOT NULL DEFAULT 'never'
        CHECK (stop_policy_kind IN ('never', 'after_consecutive_failures')),
    stop_policy_data JSON,

    consecutive_failures INTEGER NOT NULL DEFAULT 0,

    last_success_at  TIMESTAMPTZ,
    last_failure_at  TIMESTAMPTZ,
    last_attempt_at  TIMESTAMPTZ,
    next_planned_at  TIMESTAMPTZ,
    auto_stopped_at  TIMESTAMPTZ,

    effective_state  TEXT NOT NULL
        CHECK (effective_state IN ('stopped_auto', 'failing', 'unconfigured', 'paused_manual', 'active')),
    auto_stopped_reason TEXT DEFAULT NULL
        CHECK (auto_stopped_reason IN ('stop_policy', 'unrecoverable_failure')),
        
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    last_applied_flow_system_event_id INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (scope_data, flow_type)
);

/* ------------------------------ */

-- Filter by dataset_id across scopes
CREATE INDEX idx_fps_dataset_id
  ON flow_process_states (json_extract(scope_data, '$.dataset_id'));

-- Narrow to dataset-only scopes
CREATE INDEX idx_fps_dataset_only
  ON flow_process_states (json_extract(scope_data, '$.dataset_id'))
  WHERE json_extract(scope_data, '$.type') = 'Dataset';

-- Narrow to webhook-only scopes
CREATE INDEX idx_fps_webhook_only
  ON flow_process_states (json_extract(scope_data, '$.dataset_id'))
  WHERE json_extract(scope_data, '$.type') = 'WebhookSubscription';

-- Effective-state filters
CREATE INDEX idx_fps_effective_state
  ON flow_process_states (effective_state);

-- “Recent activity” sorts
CREATE INDEX idx_fps_last_attempt_desc
  ON flow_process_states (last_attempt_at DESC);

-- “Recent failures” sorts
CREATE INDEX idx_fps_last_failure_desc
  ON flow_process_states (last_failure_at DESC);

-- “Next planned” sorts and filters for updates-only flows
CREATE INDEX idx_fps_updates_next
  ON flow_process_states (next_planned_at)
  WHERE flow_type IN ('dev.kamu.flow.dataset.ingest','dev.kamu.flow.dataset.transform');

/* ------------------------------ */

DROP INDEX IF EXISTS idx_flow_events_finished_by_flow;

/* ------------------------------ */

