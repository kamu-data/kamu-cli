CREATE TABLE dataset_env_vars_tmp(
    id VARCHAR(36) NOT NULL PRIMARY KEY,
    key VARCHAR(200) NOT NULL,
    value BLOB NOT NULL,
    secret_nonce BLOB,
    created_at timestamptz NOT NULL,
    dataset_id VARCHAR(100) NOT NULL,
    CONSTRAINT dataset_env_var_dataset_entry
        FOREIGN KEY (dataset_id)
        REFERENCES dataset_entries(dataset_id)
        ON DELETE CASCADE
);

DROP INDEX idx_dataset_env_vars_key_dataset;
CREATE UNIQUE INDEX idx_dataset_env_vars_key_dataset ON dataset_env_vars_tmp(dataset_id, key);
DROP INDEX idx_dataset_env_vars_dataset_id;
CREATE INDEX idx_dataset_env_vars_dataset_id ON dataset_env_vars_tmp(dataset_id);

INSERT INTO dataset_env_vars_tmp (id, key, value, secret_nonce, created_at, dataset_id)
SELECT id, key, value, secret_nonce, created_at, dataset_id
FROM dataset_env_vars;

DROP TABLE dataset_env_vars;
ALTER TABLE dataset_env_vars_tmp
RENAME TO dataset_env_vars;