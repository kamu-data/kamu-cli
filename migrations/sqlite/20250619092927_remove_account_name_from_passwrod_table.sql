/* ------------------------------ */

DROP INDEX IF EXISTS idx_accounts_passwords_account_name;

ALTER TABLE accounts_passwords
    DROP COLUMN account_name;

/* ------------------------------ */
