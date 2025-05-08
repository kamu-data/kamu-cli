/* ------------------------------ */

CREATE TYPE did_entity_type AS ENUM ('account', 'dataset');

/* ------------------------------ */

CREATE TABLE did_secret_keys(
   entity_type  did_entity_type NOT NULL,
   entity_id    VARCHAR(100) NOT NULL,
   secret_nonce BYTEA NOT NULL,
   secret_key   BYTEA NOT NULL,
   creator_id   VARCHAR(100) NOT NULL REFERENCES accounts(id) ON DELETE CASCADE
);

CREATE INDEX idx_auth_did_secret_keys
    ON did_secret_keys (entity_type, entity_id);

/* ------------------------------ */
