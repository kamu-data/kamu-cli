/* ------------------------------ */

-- Every dataset gets a stable resource id: the id it keeps once datasets
-- become resources, as task specs refer to datasets by it.

ALTER TABLE dataset_entries
    ADD COLUMN resource_id UUID;

UPDATE dataset_entries
    SET resource_id = gen_random_uuid()
    WHERE resource_id IS NULL;

ALTER TABLE dataset_entries
    ALTER COLUMN resource_id SET NOT NULL;

CREATE UNIQUE INDEX idx_dataset_entries_resource_id ON dataset_entries (resource_id);

/* ------------------------------ */
