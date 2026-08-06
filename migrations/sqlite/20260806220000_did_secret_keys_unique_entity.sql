/* ------------------------------ */

-- Deduplicate existing rows (keep the oldest by rowid).
DELETE
FROM did_secret_keys
WHERE rowid NOT IN (SELECT MIN(rowid)
                    FROM did_secret_keys
                    GROUP BY entity_type, entity_id);

DROP INDEX idx_auth_did_secret_keys;

CREATE UNIQUE INDEX idx_auth_did_secret_keys
    ON did_secret_keys (entity_type, entity_id);

/* ------------------------------ */
