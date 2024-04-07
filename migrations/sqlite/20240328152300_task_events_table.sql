CREATE TABLE tasks (
    task_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    created_time timestamptz NOT NULL
);

CREATE TABLE task_events (
    event_id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    task_id BIGINT NOT NULL,
    dataset_id VARCHAR(100),
    event_time timestamptz NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE INDEX task_events_task_id_idx ON task_events (task_id);
CREATE INDEX task_events_dataset_id_idx On task_events(dataset_id);