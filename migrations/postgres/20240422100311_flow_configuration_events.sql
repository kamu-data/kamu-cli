CREATE SEQUENCE flow_configuration_event_id_seq AS BIGINT;

-- Add a value to enum:
-- ALTER TYPE system_flow_type ADD VALUE 'new_value';
--
-- Rename an enum value:
-- ALTER TYPE system_flow_type RENAME VALUE 'existing_value' TO 'new_value';
CREATE TYPE system_flow_type AS ENUM ('gc');

CREATE TABLE system_flow_configuration_events
(
    event_id         BIGINT PRIMARY KEY DEFAULT NEXTVAL('flow_configuration_event_id_seq'),
    system_flow_type system_flow_type NOT NULL,
    event_type       VARCHAR(50)      NOT NULL,
    event_time       TIMESTAMPTZ      NOT NULL,
    event_payload    JSONB            NOT NULL
);

-- Add a value to enum:
-- ALTER TYPE dataset_flow_type ADD VALUE 'new_value';
--
-- Rename an enum value:
-- ALTER TYPE dataset_flow_type RENAME VALUE 'existing_value' TO 'new_value';
CREATE TYPE dataset_flow_type AS ENUM ('ingest', 'execute_transform', 'hard_compaction');

CREATE TABLE dataset_flow_configuration_events
(
    event_id          BIGINT PRIMARY KEY DEFAULT NEXTVAL('flow_configuration_event_id_seq'),
    dataset_id        VARCHAR(100)      NOT NULL,
    dataset_flow_type dataset_flow_type NOT NULL,
    event_type        VARCHAR(50)       NOT NULL,
    event_time        TIMESTAMPTZ       NOT NULL,
    event_payload     JSONB             NOT NULL
);

CREATE INDEX dataset_flow_configuration_events_dataset_id_idx ON dataset_flow_configuration_events (dataset_id, dataset_flow_type);
