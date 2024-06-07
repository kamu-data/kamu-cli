CREATE TABLE access_tokens (
    id CHAR(36) DEFAULT (UUID()),
    token_name VARCHAR(100) NOT NULL,
    token_hash BINARY(32) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    revoked_at TIMESTAMP NULL,
    account_id VARCHAR(100),
    CONSTRAINT fk_account_id FOREIGN KEY (account_id) REFERENCES accounts(id),
    UNIQUE KEY idx_account_token_name (account_id, token_name)
);
