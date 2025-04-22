/* ------------------------------ */

ALTER TABLE dataset_entries
    ADD COLUMN owner_name VARCHAR(100);

UPDATE dataset_entries
SET owner_name = (
    SELECT account_name
    FROM accounts
    WHERE accounts.id = dataset_entries.owner_id
);


ALTER TABLE dataset_entries
    ALTER COLUMN owner_name SET NOT NULL;    

/* ------------------------------ */
