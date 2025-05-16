/* ------------------------------ */

-- Create a new table
CREATE TABLE accounts_passwords_new
(
    account_name  VARCHAR(100) NOT NULL,
    password_hash VARCHAR(100) NOT NULL,
    account_id    VARCHAR(100) NOT NULL REFERENCES accounts (id) ON DELETE CASCADE
);

-- Copying rows
INSERT INTO accounts_passwords_new (account_name,
                                    password_hash,
                                    account_id)
SELECT p.account_name,
       p.password_hash,
       a.id
FROM accounts_passwords p
         JOIN accounts a ON a.account_name = p.account_name
WHERE p.account_name = a.account_name;

-- Deleting the old table
DROP TABLE accounts_passwords;

-- Turning the new table into a main table
ALTER TABLE accounts_passwords_new
    RENAME TO accounts_passwords;

-- Re-create the index
CREATE UNIQUE INDEX idx_accounts_passwords_account_name ON accounts_passwords (LOWER(account_name));

/* ------------------------------ */
