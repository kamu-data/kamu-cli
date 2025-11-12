/* ------------------------------ */

ALTER TABLE dataset_statistics
ADD COLUMN num_object_links BIGINT NOT NULL DEFAULT 0,
ADD COLUMN object_links_size BIGINT NOT NULL DEFAULT 0;

/* ------------------------------ */
