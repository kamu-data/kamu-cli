/* ------------------------------ */

CREATE SEQUENCE task_event_id_seq AS BIGINT;
CREATE SEQUENCE task_id_seq AS BIGINT;

/* ------------------------------ */

CREATE TYPE task_status_type AS ENUM ('queued', 'running', 'finished');

/* ------------------------------ */

CREATE TABLE tasks
(
    task_id               BIGINT NOT NULL PRIMARY KEY,
    dataset_id            VARCHAR(100),
    task_status           task_status_type NOT NULL
);

CREATE INDEX idx_tasks_dataset_id ON tasks (dataset_id) WHERE dataset_id IS NOT NULL;
CREATE INDEX idx_tasks_task_status ON tasks(task_status) WHERE task_status != 'finished';

/* ------------------------------ */

CREATE TABLE task_events (
    event_id BIGINT PRIMARY KEY DEFAULT NEXTVAL('task_event_id_seq'),
    task_id BIGINT NOT NULL REFERENCES tasks(task_id),
    event_time timestamptz NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE INDEX idx_task_events_task_id ON task_events (task_id);

/* ------------------------------ */
