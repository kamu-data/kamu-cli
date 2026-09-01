/* ------------------------------ */

-- Every account gets an artificial, stable resource id (UUID). This is the id of
-- the account *resource* that `Account` will eventually be a projection of. For
-- now it is only stored and threaded into `AccountHandle`; no account-resource
-- history is synthesized yet.
--
-- MySQL has no native UUID column type; the id is stored as a 36-char string and
-- backfilled via the built-in UUID() generator.

-- 1. Add nullable column.
ALTER TABLE accounts
    ADD COLUMN resource_id CHAR(36);

-- 2. Backfill existing rows with random UUIDs.
UPDATE accounts
    SET resource_id = UUID()
    WHERE resource_id IS NULL;

-- 3. Enforce NOT NULL now that every row is populated.
ALTER TABLE accounts
    MODIFY COLUMN resource_id CHAR(36) NOT NULL;

-- 4. Resource ids are globally unique.
CREATE UNIQUE INDEX idx_accounts_resource_id ON accounts (resource_id);

/* ------------------------------ */
