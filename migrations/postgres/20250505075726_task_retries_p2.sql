/* ------------------------------ */

CREATE INDEX idx_tasks_queued_retrying
  ON tasks (task_status, next_attempt_at, task_id)
  WHERE task_status IN ('queued', 'retrying');

/* ------------------------------ */
