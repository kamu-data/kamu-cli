/* ------------------------------ */

-- Add paused_at and running_since columns to track process state transitions

ALTER TABLE flow_process_states
    ADD COLUMN paused_at TIMESTAMPTZ DEFAULT NULL,
    ADD COLUMN running_since TIMESTAMPTZ DEFAULT NULL;

/* ------------------------------ */
