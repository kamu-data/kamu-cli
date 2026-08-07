/* ------------------------------ */

-- Deduplicate existing rows (keep the oldest by rowid).
DELETE
FROM did_secret_keys
WHERE rowid NOT IN (SELECT MIN(rowid)
                    FROM did_secret_keys
                    GROUP BY entity_type, entity_id);

-- NOTE: Leave the previous index as is:
--       idx_auth_did_secret_keys (entity_type, entity_id);

CREATE UNIQUE INDEX idx_auth_did_secret_keys_unique_entity_id
    ON did_secret_keys (entity_id);


/* ------------------------------ */
