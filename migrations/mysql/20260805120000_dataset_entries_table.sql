/* ------------------------------ */

CREATE TABLE dataset_entries
(
    dataset_id         VARCHAR(100)                NOT NULL PRIMARY KEY,
    owner_id           VARCHAR(100)                NOT NULL,
    owner_name         VARCHAR(100)                NOT NULL,
    dataset_name       VARCHAR(100)                NOT NULL,
    -- NOTE: MariaDB has no functional indexes; use a generated column instead of LOWER(dataset_name).
    dataset_name_lower VARCHAR(100) AS (LOWER(dataset_name)) VIRTUAL,
    created_at         TIMESTAMP(6)                NOT NULL,
    kind               ENUM ('root', 'derivative') NOT NULL
);

CREATE INDEX idx_dataset_entries_owner_id
    ON dataset_entries (owner_id);

CREATE UNIQUE INDEX idx_dataset_entries_owner_id_dataset_name
    ON dataset_entries (owner_id, dataset_name_lower);

/* ------------------------------ */
