/* ------------------------------ */

CREATE TABLE did_secret_keys(
   entity_type  VARCHAR(25)  CHECK (entity_type IN ('account', 'dataset')) NOT NULL,
   entity_id    VARCHAR(100) NOT NULL,
   secret_nonce BLOB NOT NULL,
   secret_key BLOB NOT NULL,
   creator_id   VARCHAR(100) NOT NULL REFERENCES accounts(id) ON DELETE CASCADE
);

CREATE INDEX idx_auth_did_secret_keys
    ON did_secret_keys (entity_type, entity_id);

/* ------------------------------ */
