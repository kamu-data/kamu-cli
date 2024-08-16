CREATE TABLE flow_configuration_event
(
    event_id     INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    created_time timestamptz                       NOT NULL
);

CREATE TABLE system_flow_configuration_events
(
    event_id         INTEGER PRIMARY KEY                              NOT NULL,
    system_flow_type VARCHAR(10) CHECK ( system_flow_type IN ('gc') ) NOT NULL,
    event_type       VARCHAR(50)                                      NOT NULL,
    event_time       TIMESTAMPTZ                                      NOT NULL,
    event_payload    JSONB                                            NOT NULL
);

CREATE TABLE dataset_flow_configuration_events
(
    event_id          INTEGER PRIMARY KEY                                                                                    NOT NULL,
    dataset_id        VARCHAR(100)                                                                                           NOT NULL,
    dataset_flow_type VARCHAR(20) CHECK ( dataset_flow_type IN ('ingest', 'execute_transform', 'hard_compaction', 'reset') ) NOT NULL,
    event_type        VARCHAR(50)                                                                                            NOT NULL,
    event_time        TIMESTAMPTZ                                                                                            NOT NULL,
    event_payload     JSONB                                                                                                  NOT NULL
);

CREATE INDEX dataset_flow_configuration_events_dataset_id_idx ON dataset_flow_configuration_events (dataset_id, dataset_flow_type);
