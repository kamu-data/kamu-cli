/* ------------------------------ */

CREATE TABLE access_tokens(
    id uuid NOT NULL PRIMARY KEY,
    token_name VARCHAR(100) NOT NULL,
    token_hash BYTEA NOT NULL,
    created_at timestamptz NOT NULL,
    revoked_at timestamptz,
    last_used_at timestamptz,
    account_id VARCHAR(100) NOT NULL REFERENCES accounts(id)
);

CREATE UNIQUE INDEX idx_access_tokens_account_id_token_name
    ON access_tokens(account_id, token_name) 
    WHERE revoked_at IS NULL;

/* ------------------------------ */
