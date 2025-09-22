/* ------------------------------ */

CREATE TYPE flow_stop_policy_kind AS ENUM (
    'never',
    'after_consecutive_failures'
);

/* ------------------------------ */

CREATE TYPE flow_process_effective_state AS ENUM (
    'stopped_auto',
    'failing',
    'paused_manual',
    'active'
);

/* ------------------------------ */

CREATE TABLE flow_process_states (
    scope_data      JSONB NOT NULL,
    flow_type       VARCHAR(100) NOT NULL,

    paused_manual   BOOLEAN NOT NULL DEFAULT false,

    stop_policy_kind flow_stop_policy_kind NOT NULL DEFAULT 'never',
    stop_policy_data JSONB,

    consecutive_failures INT NOT NULL DEFAULT 0,

    last_success_at  TIMESTAMPTZ,
    last_failure_at  TIMESTAMPTZ,
    last_attempt_at  TIMESTAMPTZ,
    next_planned_at  TIMESTAMPTZ,

    effective_state  flow_process_effective_state NOT NULL,

    updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_applied_flow_system_event_id BIGINT NOT NULL DEFAULT 0,

    CONSTRAINT flow_process_state_pkey PRIMARY KEY (scope_data, flow_type)
);

/* ------------------------------ */

-- By dataset_id across all scopes (dataset + webhooks)
CREATE INDEX idx_fps_dataset_id ON flow_process_states ((scope_data->>'dataset_id'));

-- Dataset-only scopes 
CREATE INDEX idx_fps_dataset_only ON flow_process_states ((scope_data->>'dataset_id'))
  WHERE (scope_data->>'type') = 'Dataset';

-- Webhook-only scopes
CREATE INDEX idx_fps_webhook_only ON flow_process_states ((scope_data->>'dataset_id'))
  WHERE (scope_data->>'type') = 'WebhookSubscription';

-- Effective state filters
CREATE INDEX idx_fps_effective_state ON flow_process_states (effective_state);

-- “Recent activity” sorts
CREATE INDEX idx_fps_last_attempt_desc ON flow_process_states (last_attempt_at DESC);

-- “Recent failures” sorts
CREATE INDEX idx_fps_last_failure_desc
  ON flow_process_states (last_failure_at DESC);

-- “Next planned” sorts and filters for updates-only flows
CREATE INDEX idx_fps_updates_next
  ON flow_process_states (next_planned_at)
  WHERE flow_type IN ('dev.kamu.flow.dataset.ingest','dev.kamu.flow.dataset.transform');

/* ------------------------------ */

DROP INDEX IF EXISTS idx_flow_events_finished_by_flow_desc;

/* ------------------------------ */
