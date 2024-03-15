CREATE SEQUENCE task_id_seq AS BIGINT;

CREATE TABLE task_events (
    task_event_id BIGSERIAL,
    PRIMARY KEY (task_event_id),
    task_id BIGINT NOT NULL,
    dataset_id TEXT,
    event_time timestamptz NOT NULL,
    event_type TEXT NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE INDEX task_events_task_id_idx ON task_events (task_id);
CREATE INDEX task_events_dataset_id_idx On task_events(dataset_id);