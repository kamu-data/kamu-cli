/* ------------------------------ */

-- This migration is rather complex, as we have to modify `task_status` CHECK constraint.
-- This is not possible in SQLite directly, so we have to create new tables and copy data over.

-- Rename existing tables to "old" versions

ALTER TABLE tasks RENAME TO tasks_old;
ALTER TABLE task_events RENAME TO task_events_old;

-- Create new tables with the updated constraints

CREATE TABLE task_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    task_id BIGINT NOT NULL REFERENCES task_ids(task_id),
    event_time timestamptz NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_payload JSONB NOT NULL
);

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

-- Copy data from old tables to new tables

INSERT INTO task_events (event_id, task_id, event_time, event_type, event_payload)
    SELECT event_id, task_id, event_time, event_type, event_payload
    FROM task_events_old;    


INSERT INTO tasks (task_id, dataset_id, task_status, last_event_id, next_attempt_at)
    SELECT task_id, dataset_id, task_status, last_event_id, NULL
    FROM tasks_old;

-- Drop old tables. Since they are cross-dependent, get rid of first foreign key separately

DROP INDEX idx_task_events_task_id;

ALTER TABLE task_events_old DROP COLUMN task_id;

DROP TABLE tasks_old;

DROP TABLE task_events_old;

-- Recreate indexes

CREATE INDEX idx_task_events_task_id ON task_events (task_id);

CREATE INDEX idx_tasks_dataset_id ON tasks (dataset_id) WHERE dataset_id IS NOT NULL;
CREATE INDEX idx_tasks_task_status ON tasks (task_status) WHERE task_status != 'finished';

/* ------------------------------ */

-- New index for retrying tasks

CREATE INDEX idx_tasks_queued_retrying
  ON tasks (task_status, next_attempt_at, task_id)
  WHERE task_status IN ('queued', 'retrying');

/* ------------------------------ */
