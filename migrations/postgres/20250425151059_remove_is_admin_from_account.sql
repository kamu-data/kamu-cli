INSERT INTO auth_rebac_properties (entity_type, entity_id, property_name, property_value)
SELECT 
  'account' AS entity_type,
  id::VARCHAR AS entity_id,
  'is_admin' AS property_name,
  is_admin::TEXT AS property_value
FROM accounts;

ALTER TABLE accounts
DROP COLUMN is_admin;