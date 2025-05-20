/* ------------------------------ */

-- Temporarily disable FK validation
PRAGMA foreign_keys = OFF;

-- Create a new table
CREATE TABLE dataset_entries_new
(
    dataset_id   VARCHAR(100) NOT NULL PRIMARY KEY,
    owner_id     VARCHAR(100) NOT NULL REFERENCES accounts (id) ON DELETE CASCADE,
    dataset_name VARCHAR(100) NOT NULL,
    created_at   timestamptz  NOT NULL,
    kind         VARCHAR(15)  NOT NULL CHECK (kind IN ('root', 'derivative')),
    owner_name   VARCHAR(100) NOT NULL
);

-- Copying rows
INSERT INTO dataset_entries_new (dataset_id,
                                 owner_id,
                                 dataset_name,
                                 created_at,
                                 kind,
                                 owner_name)
SELECT dataset_id,
       owner_id,
       dataset_name,
       created_at,
       kind,
       owner_name
FROM dataset_entries;

-- Deleting the old table
DROP TABLE dataset_entries;

-- Turning the new table into a main table
ALTER TABLE dataset_entries_new
    RENAME TO dataset_entries;

-- Re-create indexes
CREATE INDEX idx_dataset_entries_owner_id
    ON dataset_entries (owner_id);

CREATE UNIQUE INDEX idx_dataset_entries_owner_id_dataset_name
    ON dataset_entries (owner_id, dataset_name);

-- Turn FK validation back on
PRAGMA foreign_keys = ON;

/* ------------------------------ */
