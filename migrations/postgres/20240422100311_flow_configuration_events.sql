/* ------------------------------ */

CREATE SEQUENCE flow_configuration_event_id_seq AS BIGINT;

/* ------------------------------ */

-- Add a value to enum:
-- ALTER TYPE system_flow_type ADD VALUE 'new_value';
--
-- Rename an enum value:
-- ALTER TYPE system_flow_type RENAME VALUE 'existing_value' TO 'new_value';
CREATE TYPE system_flow_type AS ENUM ('gc');

-- Add a value to enum:
-- ALTER TYPE dataset_flow_type ADD VALUE 'new_value';
--
-- Rename an enum value:
-- ALTER TYPE dataset_flow_type RENAME VALUE 'existing_value' TO 'new_value';
CREATE TYPE dataset_flow_type AS ENUM ('ingest', 'execute_transform', 'hard_compaction', 'reset');

/* ------------------------------ */

CREATE TABLE flow_configuration_events
(
    event_id          BIGINT PRIMARY KEY DEFAULT NEXTVAL('flow_configuration_event_id_seq'),
    dataset_id        VARCHAR(100),
    dataset_flow_type dataset_flow_type,
    system_flow_type  system_flow_type,
    event_type        VARCHAR(50)       NOT NULL,
    event_time        TIMESTAMPTZ       NOT NULL,
    event_payload     JSONB             NOT NULL
);

CREATE INDEX idx_flow_configuration_events_dataset_flow_key 
   ON flow_configuration_events (dataset_id, dataset_flow_type)
   WHERE dataset_id IS NOT NULL;
   
CREATE INDEX idx_flow_configuration_events_system_flow_key
   ON flow_configuration_events (system_flow_type) 
   WHERE system_flow_type IS NOT NULL;

/* ------------------------------ */
