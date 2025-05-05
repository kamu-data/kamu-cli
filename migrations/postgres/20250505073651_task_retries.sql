/* ------------------------------ */

ALTER TYPE task_status_type
  ADD VALUE 'retrying' AFTER 'running';

/* ------------------------------ */

ALTER TABLE tasks ADD COLUMN next_attempt_at timestamptz;

/* ------------------------------ */
