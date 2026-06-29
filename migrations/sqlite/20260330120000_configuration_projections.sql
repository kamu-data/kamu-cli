/* ------------------------------ */

CREATE TABLE config_variable_set_entries (
    entry_id               CHAR(36) NOT NULL,
    resource_id            CHAR(36) NOT NULL,
    resource_generation    BIGINT NOT NULL,
    account_id             VARCHAR(100) NOT NULL,
    variable_key           VARCHAR(200) NOT NULL,
    value                  TEXT NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_config_variable_set_entries_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id) ON DELETE CASCADE,

    CONSTRAINT uq_config_variable_set_entries_resource_generation_key
        UNIQUE (resource_id, resource_generation, variable_key)
);

CREATE INDEX idx_config_variable_set_entries_resource_id_generation
    ON config_variable_set_entries (resource_id, resource_generation);

CREATE INDEX idx_config_variable_set_entries_account_key
    ON config_variable_set_entries (account_id, variable_key);

/* ------------------------------ */

CREATE TABLE config_secret_set_entries (
    entry_id               CHAR(36) NOT NULL,
    resource_id            CHAR(36) NOT NULL,
    resource_generation    BIGINT NOT NULL,
    account_id             VARCHAR(100) NOT NULL,
    secret_key             VARCHAR(200) NOT NULL,
    value                  BLOB NOT NULL,
    secret_nonce           BLOB NOT NULL,
    created_at             TIMESTAMPTZ NOT NULL,
    updated_at             TIMESTAMPTZ NOT NULL,

    CONSTRAINT fk_config_secret_set_entries_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id) ON DELETE CASCADE,

    CONSTRAINT uq_config_secret_set_entries_resource_generation_key
        UNIQUE (resource_id, resource_generation, secret_key)
);

CREATE INDEX idx_config_secret_set_entries_resource_id_generation
    ON config_secret_set_entries (resource_id, resource_generation);

CREATE INDEX idx_config_secret_set_entries_account_key
    ON config_secret_set_entries (account_id, secret_key);

/* ------------------------------ */

CREATE TABLE config_dataset_variable_set_bindings (
    dataset_id             VARCHAR(100) NOT NULL,
    resource_id            CHAR(36) NOT NULL,
    binding_order          BIGINT NOT NULL,

    CONSTRAINT pk_config_dataset_variable_set_bindings
        PRIMARY KEY (dataset_id, binding_order),

    CONSTRAINT uq_config_dataset_variable_set_bindings_dataset_resource
        UNIQUE (dataset_id, resource_id),

    CONSTRAINT fk_config_dataset_variable_set_bindings_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id) ON DELETE CASCADE
);

CREATE INDEX idx_config_dataset_variable_set_bindings_dataset_id
    ON config_dataset_variable_set_bindings (dataset_id, binding_order);

CREATE INDEX idx_config_dataset_variable_set_bindings_resource_id
    ON config_dataset_variable_set_bindings (resource_id);

/* ------------------------------ */

CREATE TABLE config_dataset_secret_set_bindings (
    dataset_id             VARCHAR(100) NOT NULL,
    resource_id            CHAR(36) NOT NULL,
    binding_order          BIGINT NOT NULL,

    CONSTRAINT pk_config_dataset_secret_set_bindings
        PRIMARY KEY (dataset_id, binding_order),

    CONSTRAINT uq_config_dataset_secret_set_bindings_dataset_resource
        UNIQUE (dataset_id, resource_id),

    CONSTRAINT fk_config_dataset_secret_set_bindings_resource_id
        FOREIGN KEY (resource_id) REFERENCES resources(resource_id) ON DELETE CASCADE
);

CREATE INDEX idx_config_dataset_secret_set_bindings_dataset_id
    ON config_dataset_secret_set_bindings (dataset_id, binding_order);

CREATE INDEX idx_config_dataset_secret_set_bindings_resource_id
    ON config_dataset_secret_set_bindings (resource_id);

/* ------------------------------ */
