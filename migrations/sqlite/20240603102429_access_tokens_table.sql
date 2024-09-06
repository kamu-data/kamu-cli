/* ------------------------------ */

CREATE TABLE access_tokens(
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    token_name VARCHAR(100) NOT NULL,
    token_hash BLOB NOT NULL,
    created_at TEXT NOT NULL,
    revoked_at TEXT,
    last_used_at TEXT,
    account_id VARCHAR(100) NOT NULL REFERENCES accounts(id)
);

CREATE UNIQUE INDEX idx_access_tokens_account_token_name 
    ON access_tokens(account_id, token_name) 
    WHERE revoked_at IS NULL;

/* ------------------------------ */
