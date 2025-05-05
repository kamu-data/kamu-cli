/* ------------------------------ */

PRAGMA foreign_keys = OFF;

/* ------------------------------ */

ALTER TABLE tasks RENAME TO tasks_old;

CREATE TABLE tasks (
    task_id BIGINT NOT NULL PRIMARY KEY REFERENCES task_ids(task_id),
    dataset_id VARCHAR(100),
    task_status VARCHAR(10) CHECK (
        task_status IN (
           'queued', 
           'running', 
           'finished', 
           'retrying'
        )
    ) NOT NULL,
    last_event_id INTEGER REFERENCES task_events(event_id),   
    next_attempt_at timestamptz
);

INSERT INTO tasks (task_id, dataset_id, task_status, last_event_id, next_attempt_at)
    SELECT task_id, dataset_id, task_status, last_event_id, NULL
    FROM tasks_old;

DROP TABLE tasks_old;

/* ------------------------------ */

PRAGMA foreign_keys = ON;

/* ------------------------------ */

CREATE INDEX idx_tasks_queued_retrying
  ON tasks (task_status, next_attempt_at, task_id)
  WHERE task_status IN ('queued', 'retrying');

/* ------------------------------ */
