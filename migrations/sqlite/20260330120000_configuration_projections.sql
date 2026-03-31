/* ------------------------------ */

CREATE TABLE config_variable_set_entries (
    entry_id               CHAR(36) PRIMARY KEY NOT NULL,
    resource_uid           CHAR(36) NOT NULL,
    resource_generation    BIGINT NOT NULL,
    account_id             VARCHAR(100) NOT NULL,
    variable_key           VARCHAR(200) NOT NULL,
    value                  TEXT NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_config_variable_set_entries_resource_uid
        FOREIGN KEY (resource_uid) REFERENCES resources(resource_uid) ON DELETE CASCADE,

    CONSTRAINT uq_config_variable_set_entries_resource_generation_key
        UNIQUE (resource_uid, resource_generation, variable_key)
);

CREATE INDEX idx_config_variable_set_entries_resource_uid_generation
    ON config_variable_set_entries (resource_uid, resource_generation);

CREATE INDEX idx_config_variable_set_entries_account_key
    ON config_variable_set_entries (account_id, variable_key);

/* ------------------------------ */

CREATE TABLE config_secret_set_entries (
    entry_id               CHAR(36) PRIMARY KEY NOT NULL,
    resource_uid           CHAR(36) NOT NULL,
    resource_generation    BIGINT NOT NULL,
    account_id             VARCHAR(100) NOT NULL,
    secret_key             VARCHAR(200) NOT NULL,
    value                  BLOB NOT NULL,
    secret_nonce           BLOB NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_config_secret_set_entries_resource_uid
        FOREIGN KEY (resource_uid) REFERENCES resources(resource_uid) ON DELETE CASCADE,

    CONSTRAINT uq_config_secret_set_entries_resource_generation_key
        UNIQUE (resource_uid, resource_generation, secret_key)
);

CREATE INDEX idx_config_secret_set_entries_resource_uid_generation
    ON config_secret_set_entries (resource_uid, resource_generation);

CREATE INDEX idx_config_secret_set_entries_account_key
    ON config_secret_set_entries (account_id, secret_key);

/* ------------------------------ */
