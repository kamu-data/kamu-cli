/* ------------------------------ */

ALTER TABLE accounts_passwords
    ADD COLUMN account_id VARCHAR(100);

UPDATE accounts_passwords p
SET account_id = a.id
FROM accounts a
WHERE p.account_name = a.account_name;

ALTER TABLE accounts_passwords
    ALTER COLUMN account_id SET NOT NULL;

ALTER TABLE accounts_passwords
    ADD CONSTRAINT fk_accounts_passwords_accounts_id
        FOREIGN KEY (account_id)
            REFERENCES accounts (id)
            ON DELETE CASCADE;

/* ------------------------------ */
