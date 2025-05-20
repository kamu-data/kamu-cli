/* ------------------------------ */

-- "dataset_entries_owner_id_fkey" is obtained by a query:
-- SELECT conname
-- FROM pg_constraint
-- WHERE conrelid = 'dataset_entries'::regclass AND contype = 'f';

ALTER TABLE dataset_entries
    DROP CONSTRAINT dataset_entries_owner_id_fkey;

ALTER TABLE dataset_entries
    ADD CONSTRAINT fk_dataset_entries_owner_id
        FOREIGN KEY (owner_id)
            REFERENCES accounts (id)
            ON DELETE CASCADE;

/* ------------------------------ */
