DROP INDEX idx_account_token_name;

CREATE UNIQUE INDEX idx_account_token_name
ON access_tokens (account_id, token_name)
WHERE revoked_at IS NULL;
