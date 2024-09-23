/* ------------------------------ */

CREATE TABLE dataset_env_vars(
    id UUID PRIMARY KEY,
    key VARCHAR(200) NOT NULL,
    value BYTEA NOT NULL,
    secret_nonce BYTEA,
    created_at timestamptz NOT NULL,
    dataset_id VARCHAR(100) NOT NULL
);

CREATE UNIQUE INDEX idx_dataset_env_vars_dataset_id_key ON dataset_env_vars(dataset_id, key);

CREATE INDEX idx_dataset_env_vars_dataset_id ON dataset_env_vars(dataset_id);

/* ------------------------------ */
