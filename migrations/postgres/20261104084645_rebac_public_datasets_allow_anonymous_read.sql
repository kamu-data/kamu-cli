-- Public datasets were created with anonymous read off, which nothing enforced.
-- Anonymous read is enforced now, so open every public dataset to anonymous
-- readers to keep their current access.
INSERT INTO auth_rebac_properties (entity_type, entity_id, property_name, property_value)
SELECT entity_type, entity_id, 'dataset/allows_anonymous_read', 'true'
FROM auth_rebac_properties
WHERE entity_type = 'dataset'
  AND property_name = 'dataset/allows_public_read'
  AND property_value = 'true'
ON CONFLICT (entity_type, entity_id, property_name)
    DO UPDATE SET property_value = 'true';
