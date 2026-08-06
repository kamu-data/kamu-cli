/* ------------------------------ */

-- Deduplicate existing rows (keep the oldest by ctid).
DELETE
FROM did_secret_keys t
    USING (SELECT ctid,
                  ROW_NUMBER() OVER (PARTITION BY entity_type, entity_id ORDER BY ctid) AS rn
           FROM did_secret_keys) dup
WHERE t.ctid = dup.ctid
  AND dup.rn > 1;

DROP INDEX idx_auth_did_secret_keys;

CREATE UNIQUE INDEX idx_auth_did_secret_keys
    ON did_secret_keys (entity_type, entity_id);

/* ------------------------------ */
