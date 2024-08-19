/* ------------------------------ */

CREATE SEQUENCE flow_event_id_seq AS BIGINT;
CREATE SEQUENCE flow_id_seq AS BIGINT;

/* ------------------------------ */

CREATE TYPE flow_status_type AS ENUM ('waiting', 'running', 'finished');

/* ------------------------------ */

CREATE TABLE flows
(
    flow_id               BIGINT NOT NULL PRIMARY KEY,
    dataset_id            VARCHAR(100),
    dataset_flow_type     dataset_flow_type,
    system_flow_type      system_flow_type,
    initiator             VARCHAR(100) NOT NULL,  /* No referential integrity with account_id, as it can system initiator value */
    flow_status           flow_status_type NOT NULL
);

CREATE INDEX idx_flows_dataset_id ON flows (dataset_id) WHERE dataset_id IS NOT NULL;
CREATE INDEX idx_flows_system_flow_type ON flows (system_flow_type) WHERE system_flow_type IS NOT NULL;
CREATE INDEX idx_flows_flow_status ON flows(flow_status) WHERE flow_status != 'finished'; 

/* ------------------------------ */

CREATE TABLE flow_events
(
    event_id      BIGINT PRIMARY KEY DEFAULT NEXTVAL('flow_event_id_seq'),
    flow_id       BIGINT NOT NULL REFERENCES flows(flow_id),
    event_type    VARCHAR(50) NOT NULL,
    event_time    TIMESTAMPTZ NOT NULL,
    event_payload JSONB NOT NULL
);

CREATE INDEX idx_flow_events_flow_id ON flow_events (flow_id);

/* ------------------------------ */
