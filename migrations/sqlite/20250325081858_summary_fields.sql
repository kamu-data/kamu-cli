/* ------------------------------ */

ALTER TABLE dataset_entries
    ADD COLUMN kind VARCHAR(15) NOT NULL 
        DEFAULT 'root' 
        CHECK (kind IN ('root', 'derivative'));

UPDATE dataset_entries
    SET kind = 'derivative'
    WHERE dataset_id IN (
        SELECT downstream_dataset_id FROM dataset_dependencies
    );

/* ------------------------------ */