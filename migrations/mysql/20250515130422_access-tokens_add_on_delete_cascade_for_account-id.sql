/* ------------------------------ */

-- "access_tokens_ibfk_1" is obtained by a query:
# SELECT constraint_name
# FROM information_schema.key_column_usage
# WHERE table_name = 'access_tokens'
#   AND column_name = 'account_id'
#   AND referenced_table_name = 'accounts'
#   AND referenced_column_name = 'id';

-- Delete the old constraint ...
ALTER TABLE access_tokens
    DROP FOREIGN KEY access_tokens_ibfk_1;

-- ... and create a new one
ALTER TABLE access_tokens
    ADD CONSTRAINT fk_access_tokens_accounts_id
        FOREIGN KEY (account_id)
            REFERENCES accounts (id)
            ON DELETE CASCADE;

/* ------------------------------ */
