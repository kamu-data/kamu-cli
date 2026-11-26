/* ------------------------------ */

-- Every dataset gets a stable resource id: the id it keeps once datasets
-- become resources, as task specs refer to datasets by it.
--
-- Declared `CHAR(36)` like every other resource id column, but `sqlx` stores
-- `uuid::Uuid` in SQLite as a 16-byte blob, and affinity never converts a
-- blob, so the backfill writes random 16-byte blobs. SQLite cannot add
-- NOT NULL to an existing column; the backfill fills every row and the write
-- model always supplies an id.

ALTER TABLE dataset_entries
    ADD COLUMN resource_id CHAR(36);

UPDATE dataset_entries
    SET resource_id = randomblob(16)
    WHERE resource_id IS NULL;

CREATE UNIQUE INDEX idx_dataset_entries_resource_id ON dataset_entries (resource_id);

/* ------------------------------ */
