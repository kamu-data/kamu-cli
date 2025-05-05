/* ------------------------------ */

DELETE FROM auth_rebac_properties
  WHERE entity_type = 'account';

INSERT INTO auth_rebac_properties (entity_type, entity_id, property_name, property_value)
  SELECT 
    'account' AS entity_type,
    id AS entity_id,
    'account/is_admin' AS property_name,
    CASE is_admin
      WHEN 1 THEN 'true'
      ELSE 'false'
    END AS property_value
  FROM accounts;


ALTER TABLE accounts
  DROP COLUMN is_admin;

/* ------------------------------ */