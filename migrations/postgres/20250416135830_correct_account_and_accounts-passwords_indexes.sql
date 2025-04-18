DROP INDEX idx_accounts_name;
CREATE UNIQUE INDEX idx_accounts_name ON accounts (LOWER(account_name));

DROP INDEX idx_accounts_email;
CREATE UNIQUE INDEX idx_accounts_email ON accounts (LOWER(email));

DROP INDEX idx_accounts_passwords_account_name;
CREATE UNIQUE INDEX idx_accounts_passwords_account_name ON accounts_passwords (LOWER(account_name));
