/* ------------------------------ */

-- Deduplicate existing rows (keep the oldest by ctid).
DELETE
FROM did_secret_keys t
    USING (SELECT ctid,
                  ROW_NUMBER() OVER (PARTITION BY entity_type, entity_id ORDER BY ctid) AS rn
           FROM did_secret_keys) dup
WHERE t.ctid = dup.ctid
  AND dup.rn > 1;

-- NOTE: Leave the previous index as is:
--       idx_auth_did_secret_keys (entity_type, entity_id);

CREATE UNIQUE INDEX idx_auth_did_secret_keys_unique_entity_id
    ON did_secret_keys (entity_id);

/* ------------------------------ */
