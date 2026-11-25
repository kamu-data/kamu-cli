/* NULL for resources without owners */
ALTER TABLE resources ADD COLUMN owner_references JSONB;

/* Rebuildable projection of `resources.owner_references`. The owners may be gone. */
CREATE TABLE resource_owners_projection (
    resource_id             CHAR(36) NOT NULL,
    owner_id                CHAR(36) NOT NULL,

    PRIMARY KEY (resource_id, owner_id),

    CONSTRAINT fk_resource_owners_projection_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id) ON DELETE CASCADE
);

CREATE INDEX idx_resource_owners_projection_owner_id
    ON resource_owners_projection (owner_id, resource_id);
