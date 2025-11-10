/* ------------------------------ */

DROP INDEX idx_dataset_entries_owner_id_dataset_name;
CREATE UNIQUE INDEX idx_dataset_entries_owner_id_dataset_name
    ON dataset_entries (owner_id, LOWER(dataset_name));

/* ------------------------------ */
