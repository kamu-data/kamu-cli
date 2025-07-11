/* ------------------------------ */

DROP INDEX IF EXISTS idx_flows_flow_status;

ALTER TABLE flows RENAME COLUMN flow_status TO flow_status_old;

ALTER TABLE flows ADD COLUMN flow_status VARCHAR(10) NOT NULL DEFAULT 'waiting'
    CHECK(flow_status IN ('waiting', 'running', 'finished', 'retrying'));

UPDATE flows SET flow_status = flow_status_old;

ALTER TABLE flows DROP COLUMN flow_status_old;

CREATE INDEX idx_flows_flow_status ON flows (flow_status)
    WHERE flow_status != 'finished';

/* ------------------------------ */
