/* ------------------------------ */

-- Every account gets an artificial, stable resource id (UUID). This is the id of
-- the account *resource* that `Account` will eventually be a projection of. For
-- now it is only stored and threaded into `AccountHandle`; no account-resource
-- history is synthesized yet.
--
-- SQLite has no native UUID type or generator, so the id is stored as a 36-char
-- string. The value is only ever re-parsed by `uuid::Uuid::parse_str` on read,
-- which accepts any well-formed hex in the 8-4-4-4-12 layout, so the backfill
-- just emits random hex in that layout (no need to fix the v4 version/variant
-- nibbles). This UPDATE only ever runs against pre-existing local rows — a fresh
-- workspace is empty and the write model supplies ids from Rust.

-- 1. Add nullable column.
ALTER TABLE accounts
    ADD COLUMN resource_id CHAR(36);

-- 2. Backfill existing rows with random UUIDs.
UPDATE accounts
    SET resource_id =
        lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(2))) || '-' || lower(hex(randomblob(2))) || '-' ||
        lower(hex(randomblob(6)))
    WHERE resource_id IS NULL;

-- 3. Resource ids are globally unique. (SQLite cannot add a NOT NULL constraint
--    to an existing column in place; the write model always supplies a value and
--    the backfill above populated every row, so uniqueness is the enforced
--    guarantee here.)
CREATE UNIQUE INDEX idx_accounts_resource_id ON accounts (resource_id);

/* ------------------------------ */
