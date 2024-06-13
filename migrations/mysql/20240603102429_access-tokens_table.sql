CREATE TABLE access_tokens (
    id CHAR(36) NOT NULL PRIMARY KEY,
    token_name VARCHAR(100) NOT NULL,
    token_hash BINARY(32) NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    revoked_at TIMESTAMP(6),
    account_id VARCHAR(100) NOT NULL,
    CONSTRAINT fk_account_id FOREIGN KEY (account_id) REFERENCES accounts(id),
    UNIQUE KEY idx_account_token_name (account_id, token_name)
);
