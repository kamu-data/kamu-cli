/* ------------------------------ */

CREATE TYPE dataset_kind AS ENUM ('root', 'derivative');

/* ------------------------------ */

ALTER TABLE dataset_entries
   ADD COLUMN kind dataset_kind;

UPDATE dataset_entries
SET kind = CASE
    WHEN NOT EXISTS (
        SELECT 1 FROM dataset_dependencies d
        WHERE d.downstream_dataset_id = dataset_entries.dataset_id
    ) THEN 'root'::dataset_kind
    ELSE 'derivative'::dataset_kind
END;

ALTER TABLE dataset_entries
    ALTER COLUMN kind SET NOT NULL;

/* ------------------------------ */