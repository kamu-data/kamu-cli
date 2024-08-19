/* ------------------------------ */

CREATE TABLE access_tokens (
    id CHAR(36) NOT NULL PRIMARY KEY,
    token_name VARCHAR(100) NOT NULL,
    token_hash BINARY(32) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    revoked_at TIMESTAMP(6),
    revoked_at_is_null BOOLEAN GENERATED ALWAYS AS (revoked_at IS NULL),
    account_id VARCHAR(100) NOT NULL REFERENCES accounts(id),
    UNIQUE KEY idx_account_token_name (account_id, token_name, revoked_at_is_null)
);

/* ------------------------------ */
