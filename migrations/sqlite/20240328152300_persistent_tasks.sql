/* ------------------------------ */

CREATE TABLE task_ids (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    created_time timestamptz NOT NULL
);

/* ------------------------------ */

CREATE TABLE tasks
(
    task_id BIGINT NOT NULL PRIMARY KEY REFERENCES task_ids(task_id),
    dataset_id VARCHAR(100),
    task_status VARCHAR(10) CHECK (
        task_status IN (
           'queued', 
           'running', 
           'finished'
        )
    ) NOT NULL
);

CREATE INDEX idx_tasks_dataset_id ON tasks (dataset_id) WHERE dataset_id IS NOT NULL;
CREATE INDEX idx_tasks_task_status ON tasks (task_status) WHERE task_status != 'finished';

/* ------------------------------ */

CREATE TABLE task_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    task_id BIGINT NOT NULL REFERENCES tasks(task_id),
    event_time timestamptz NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE INDEX idx_task_events_task_id ON task_events (task_id);

/* ------------------------------ */
