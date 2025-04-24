CREATE TABLE oauth_device_codes
(
    device_code            VARCHAR(36) NOT NULL PRIMARY KEY,
    device_code_created_at TEXT        NOT NULL,
    device_code_expires_at TEXT        NOT NULL,
    token_iat              INTEGER,
    token_exp              INTEGER,
    token_last_used_at     TEXT,
    account_id             VARCHAR(100) REFERENCES accounts (id) ON DELETE CASCADE
);

CREATE INDEX idx_oauth_device_codes_device_code_expires_at
    ON oauth_device_codes (DATETIME(device_code_expires_at) DESC);
