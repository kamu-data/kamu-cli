CREATE TABLE accounts 
(
    id VARCHAR(100) NOT NULL PRIMARY KEY,
    account_name VARCHAR(100) NOT NULL,
    email VARCHAR(320),
    display_name VARCHAR(200) NOT NULL,
    account_type VARCHAR(15) CHECK( account_type IN ('user', 'organization') ) NOT NULL,
    avatar_url VARCHAR(1000),
    registered_at timestamptz NOT NULL,
    is_admin INTEGER NOT NULL,
    provider VARCHAR(25) NOT NULL,
    provider_identity_key VARCHAR(100) NOT NULL
);

CREATE UNIQUE INDEX idx_accounts_name ON accounts(account_name);
CREATE UNIQUE INDEX idx_accounts_email ON accounts(email);
CREATE UNIQUE INDEX idx_provider_identity_key ON accounts(provider_identity_key);


CREATE TABLE accounts_passwords(
    account_name VARCHAR(100) NOT NULL,
    password_hash VARCHAR(100) NOT NULL
);

CREATE UNIQUE INDEX idx_accounts_passwords_account_name ON accounts_passwords(account_name);
