CREATE TABLE auth_rebac_properties
(
    entity_type    VARCHAR(25)  NOT NULL,
    entity_id      VARCHAR(100) NOT NULL,
    property_name  VARCHAR(50)  NOT NULL,
    property_value VARCHAR(50)  NOT NULL
);

CREATE INDEX idx_uniq_entity_type_entity_id ON auth_rebac_properties (entity_type, entity_id);
CREATE UNIQUE INDEX idx_uniq_entity_type_entity_id_property_name ON auth_rebac_properties (entity_type, entity_id, property_name);
