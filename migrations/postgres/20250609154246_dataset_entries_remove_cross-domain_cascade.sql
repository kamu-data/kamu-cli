/* ------------------------------ */

-- NOTE: There should be no cross-domain strong refs (and therefore cascades).

-- "dataset_entries_owner_id_fkey" is obtained by a query:
-- SELECT conname
-- FROM pg_constraint
-- WHERE conrelid = 'dataset_entries'::regclass AND contype = 'f';

ALTER TABLE dataset_entries
    DROP CONSTRAINT fk_dataset_entries_owner_id;


/* ------------------------------ */
