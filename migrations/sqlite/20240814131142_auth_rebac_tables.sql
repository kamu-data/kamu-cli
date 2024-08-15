CREATE TABLE auth_rebac_properties
(
    entity_type    VARCHAR(25)  NOT NULL,
    entity_id      VARCHAR(100) NOT NULL,
    property_name  VARCHAR(50)  NOT NULL,
    property_value VARCHAR(50)  NOT NULL
);

CREATE INDEX idx_uniq_entity
    ON auth_rebac_properties (entity_type, entity_id);

CREATE UNIQUE INDEX idx_uniq_entity_property_name
    ON auth_rebac_properties (entity_type, entity_id, property_name);

------------------------------------------------------------------------------------------------------------------------

CREATE TABLE auth_rebac_relations
(
    subject_entity_type VARCHAR(25)  NOT NULL,
    subject_entity_id   VARCHAR(100) NOT NULL,
    relationship        VARCHAR(50)  NOT NULL,
    object_entity_type  VARCHAR(25)  NOT NULL,
    object_entity_id    VARCHAR(100) NOT NULL
);

CREATE UNIQUE INDEX idx_uniq_row
    ON auth_rebac_relations (subject_entity_type, subject_entity_id, relationship, object_entity_type,
                             object_entity_id);
CREATE INDEX idx_subject_entity
    ON auth_rebac_relations (subject_entity_type, subject_entity_id);

CREATE INDEX idx_subject_entity_relationship
    ON auth_rebac_relations (subject_entity_type, subject_entity_id, relationship);

CREATE INDEX idx_subject_entity_object_entity
    ON auth_rebac_relations (subject_entity_type, subject_entity_id, object_entity_type, object_entity_id);
