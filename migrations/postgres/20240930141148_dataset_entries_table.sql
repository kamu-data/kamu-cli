/* ------------------------------ */

CREATE TABLE dataset_entries
(
    dataset_id   VARCHAR(100) NOT NULL PRIMARY KEY,
    owner_id     VARCHAR(100) NOT NULL REFERENCES accounts (id),
    dataset_name VARCHAR(100) NOT NULL,
    created_at   timestamptz  NOT NULL
);

CREATE INDEX idx_dataset_entries_owner_id
    ON dataset_entries (owner_id);

CREATE UNIQUE INDEX idx_dataset_entries_owner_id_dataset_name
    ON dataset_entries (owner_id, dataset_name);

/* ------------------------------ */
