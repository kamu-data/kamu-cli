-- Remove the old one.
DROP INDEX idx_auth_rebac_relations_row;

-- Create a new index without the "relationship" column.
CREATE UNIQUE INDEX idx_auth_rebac_relations_row
    ON auth_rebac_relations (subject_entity_type,
                             subject_entity_id,
                             object_entity_type,
                             object_entity_id);

-- It also adds two indexes for new queries:
CREATE INDEX idx_auth_rebac_relations_subject_entity
    ON auth_rebac_relations (subject_entity_type,
                             subject_entity_id);

CREATE INDEX idx_auth_rebac_relations_object_entity
    ON auth_rebac_relations (object_entity_type,
                             object_entity_id);
