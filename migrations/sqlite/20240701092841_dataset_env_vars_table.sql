CREATE TABLE dataset_env_vars(
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    key VARCHAR(200) NOT NULL,
    value BLOB NOT NULL,
    secret_nonce BLOKB,
    created_at timestamptz NOT NULL,
    dataset_id VARCHAR(100) NOT NULL
);

CREATE UNIQUE INDEX idx_env_key_dataset ON dataset_env_vars(dataset_id, key);

CREATE INDEX dataset_env_var_dataset_id_idx ON dataset_env_vars(dataset_id);