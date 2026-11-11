/*
 * Bookkeeping of the resource controller runtime: per controller and resource,
 * what the controller last handled and when it may run again. A row with
 * `not_before` set records a retry or a failure; without it, the generation and
 * status generation the controller finished with.
 */

CREATE TABLE resource_controller_state (
    controller                VARCHAR(200) NOT NULL,
    resource_id               UUID NOT NULL,
    controller_revision       INTEGER NOT NULL,
    handled_generation        BIGINT NULL,
    handled_status_generation BIGINT NULL,
    not_before                TIMESTAMPTZ NULL,
    attempts                  INTEGER NOT NULL,
    last_error                TEXT NULL,

    PRIMARY KEY (controller, resource_id),

    CONSTRAINT fk_resource_controller_state_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id)
);

CREATE INDEX idx_resource_controller_state_resource_id
    ON resource_controller_state (resource_id);

CREATE INDEX idx_resource_controller_state_not_before
    ON resource_controller_state (controller, not_before)
    WHERE not_before IS NOT NULL;
