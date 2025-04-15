CREATE TABLE device_codes
(
    device_code            VARCHAR(36) NOT NULL PRIMARY KEY,
    device_code_created_at TEXT        NOT NULL,
    device_code_expires_at TEXT        NOT NULL,
    token_iat              INTEGER,
    token_exp              INTEGER,
    token_last_used_at     TEXT,
    account_id             VARCHAR(100) REFERENCES accounts (id)
);
