CREATE TABLE device_codes
(
    device_code            uuid        NOT NULL PRIMARY KEY,
    device_code_created_at timestamptz NOT NULL,
    device_code_expires_at timestamptz NOT NULL,
    token_iat              BIGINT,
    token_exp              BIGINT,
    token_last_used_at     timestamptz,
    account_id             VARCHAR(100) REFERENCES accounts (id)
);

CREATE INDEX idx_device_codes_device_code_expires_at
    ON device_codes (device_code_expires_at DESC);
