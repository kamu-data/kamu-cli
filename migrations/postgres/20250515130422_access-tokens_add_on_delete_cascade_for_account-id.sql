/* ------------------------------ */

-- "access_tokens_account_id_fkey" is obtained by a query:
-- SELECT conname
-- FROM pg_constraint
-- WHERE conrelid = 'access_tokens'::regclass AND contype = 'f';

ALTER TABLE access_tokens
    DROP CONSTRAINT access_tokens_account_id_fkey;

ALTER TABLE access_tokens
    ADD CONSTRAINT fk_access_tokens_accounts_id
        FOREIGN KEY (account_id)
            REFERENCES accounts (id)
            ON DELETE CASCADE;

/* ------------------------------ */
