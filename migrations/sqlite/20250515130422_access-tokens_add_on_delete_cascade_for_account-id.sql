/* ------------------------------ */

-- Create a new table
CREATE TABLE access_tokens_new
(
    id           VARCHAR(36)  NOT NULL PRIMARY KEY,
    token_name   VARCHAR(100) NOT NULL,
    token_hash   BLOB         NOT NULL,
    created_at   TEXT         NOT NULL,
    revoked_at   TEXT,
    last_used_at TEXT,
    account_id   VARCHAR(100) NOT NULL REFERENCES accounts (id) ON DELETE CASCADE
);

-- Copying rows
INSERT INTO access_tokens_new (id,
                               token_name,
                               token_hash,
                               created_at,
                               revoked_at,
                               last_used_at,
                               account_id)
SELECT id,
       token_name,
       token_hash,
       created_at,
       revoked_at,
       last_used_at,
       account_id
FROM access_tokens;

-- Deleting the old table
DROP TABLE access_tokens;

-- Turning the new table into a main table
ALTER TABLE access_tokens_new
    RENAME TO access_tokens;

-- Re-create the index
CREATE UNIQUE INDEX idx_access_tokens_account_id_token_name
    ON access_tokens (account_id, token_name)
    WHERE revoked_at IS NULL;

/* ------------------------------ */
