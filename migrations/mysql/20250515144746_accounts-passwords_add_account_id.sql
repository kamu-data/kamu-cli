/* ------------------------------ */

ALTER TABLE accounts_passwords
    ADD COLUMN account_id VARCHAR(100);

UPDATE accounts_passwords p
    INNER JOIN accounts a
    ON p.account_name = a.account_name
SET p.account_id = a.id;

ALTER TABLE accounts_passwords
    MODIFY account_id VARCHAR(100) NOT NULL;

ALTER TABLE accounts_passwords
    ADD CONSTRAINT fk_accounts_passwords_accounts_id
        FOREIGN KEY (account_id)
            REFERENCES accounts (id)
            ON DELETE CASCADE;

/* ------------------------------ */
