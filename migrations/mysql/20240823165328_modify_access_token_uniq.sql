ALTER TABLE access_tokens
    ADD revoked_at_is_null BOOLEAN GENERATED ALWAYS AS (revoked_at IS NULL);

CREATE OR REPLACE UNIQUE INDEX idx_account_token_name
    ON access_tokens (account_id, token_name, revoked_at_is_null);
