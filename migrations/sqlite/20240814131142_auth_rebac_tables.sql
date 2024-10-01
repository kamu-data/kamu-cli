/* ------------------------------ */

CREATE TABLE auth_rebac_properties
(
    entity_type    VARCHAR(25)  CHECK (entity_type IN ('account', 'dataset')) NOT NULL,
    entity_id      VARCHAR(100) NOT NULL,
    property_name  VARCHAR(50)  NOT NULL,
    property_value VARCHAR(50)  NOT NULL
);

CREATE INDEX idx_auth_rebac_properties_entity
    ON auth_rebac_properties (entity_type, entity_id);

CREATE UNIQUE INDEX idx_auth_rebac_properties_entity_property_name
    ON auth_rebac_properties (entity_type, entity_id, property_name);

/* ------------------------------ */

CREATE TABLE auth_rebac_relations
(
    subject_entity_type VARCHAR(25)  CHECK (subject_entity_type IN ('account', 'dataset')) NOT NULL,
    subject_entity_id   VARCHAR(100) NOT NULL,
    relationship        VARCHAR(50)  NOT NULL,
    object_entity_type  VARCHAR(25)  CHECK (object_entity_type IN ('account', 'dataset')) NOT NULL,
    object_entity_id    VARCHAR(100) NOT NULL
);

CREATE UNIQUE INDEX idx_auth_rebac_relations_row
    ON auth_rebac_relations (
        subject_entity_type, subject_entity_id, relationship, object_entity_type, object_entity_id
    );

/* ------------------------------ */
