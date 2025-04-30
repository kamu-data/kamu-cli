PRAGMA foreign_keys = OFF;

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

CREATE TABLE accounts_new (
    id VARCHAR(100) NOT NULL PRIMARY KEY,
    account_name VARCHAR(100) NOT NULL,
    email VARCHAR(320) NOT NULL,
    display_name VARCHAR(200) NOT NULL,
    account_type TEXT NOT NULL CHECK(account_type IN ('user', 'organization')),
    avatar_url VARCHAR(1000),
    registered_at TIMESTAMP(6) NOT NULL,
    provider VARCHAR(25) NOT NULL,
    provider_identity_key VARCHAR(100) NOT NULL
);

INSERT INTO accounts_new (
    id, 
    account_name, 
    email, 
    display_name, 
    account_type, 
    avatar_url, 
    registered_at, 
    provider,
    provider_identity_key
)
    SELECT
        id,
        account_name,
        email,
        display_name,
        account_type,
        avatar_url,
        registered_at,
        provider,
        provider_identity_key
    FROM accounts;

DROP TABLE accounts;
ALTER TABLE accounts_new RENAME TO accounts;

CREATE UNIQUE INDEX idx_accounts_name ON accounts (LOWER(account_name));
CREATE UNIQUE INDEX idx_accounts_email ON accounts (LOWER(email));
CREATE UNIQUE INDEX idx_accounts_provider_identity_key ON accounts(provider_identity_key);

PRAGMA foreign_keys = ON;
