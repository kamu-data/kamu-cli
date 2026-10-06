/* ------------------------------ */

-- Upstream flows and the downstream flows they activated, projected from flow events.
-- No foreign keys: a projection must not constrain the event store it is built from.

CREATE TABLE flow_activation_links (
    upstream_flow_id   BIGINT      NOT NULL,
    downstream_flow_id BIGINT      NOT NULL,
    activated_at       TIMESTAMPTZ NOT NULL,

    PRIMARY KEY (upstream_flow_id, downstream_flow_id)
);

/* ------------------------------ */
