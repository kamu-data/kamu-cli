/* ------------------------------ */

-- Every account gets an artificial, stable resource id (UUID). This is the id of
-- the account *resource* that `Account` will eventually be a projection of. For
-- now it is only stored and threaded into `AccountHandle`; no account-resource
-- history is synthesized yet.

-- 1. Add nullable column.
ALTER TABLE accounts
    ADD COLUMN resource_id UUID;

-- 2. Backfill existing rows with random UUIDs.
UPDATE accounts
    SET resource_id = gen_random_uuid()
    WHERE resource_id IS NULL;

-- 3. Enforce NOT NULL now that every row is populated.
ALTER TABLE accounts
    ALTER COLUMN resource_id SET NOT NULL;

-- 4. Resource ids are globally unique.
CREATE UNIQUE INDEX idx_accounts_resource_id ON accounts (resource_id);

/* ------------------------------ */
