/* ------------------------------ */

CREATE TABLE flow_process_states (
    scope_data      JSON NOT NULL,
    flow_type       TEXT NOT NULL,

    paused_manual   INTEGER NOT NULL DEFAULT 0, -- 0/1 for booleans

    stop_policy_kind TEXT NOT NULL DEFAULT 'never'
        CHECK (stop_policy_kind IN ('never', 'after_n_consecutive_failures')),
    stop_policy_data JSON,

    consecutive_failures INTEGER NOT NULL DEFAULT 0,

    last_success_at  TIMESTAMPTZ,
    last_failure_at  TIMESTAMPTZ,
    last_attempt_at  TIMESTAMPTZ,
    next_planned_at  TIMESTAMPTZ,

    effective_state  TEXT NOT NULL
        CHECK (effective_state IN ('failing', 'stopped_auto', 'paused_manual', 'active')),

    sort_key        TEXT NOT NULL,

    updated_at       TIMESTAMPTZ NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    last_applied_trigger_event_id INTEGER NOT NULL DEFAULT 0,
    last_applied_flow_event_id    INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (scope_data, flow_type)
);

/* ------------------------------ */

-- Alphabetical ordering / prefix searches on sort_key
CREATE INDEX idx_fps_sort_key_btree
  ON flow_process_states (sort_key);

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

/* ------------------------------ */
