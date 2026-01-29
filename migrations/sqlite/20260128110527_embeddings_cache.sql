/* ------------------------------ */

CREATE TABLE embedding_models (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    provider   TEXT NOT NULL,
    name       TEXT NOT NULL,
    revision   TEXT NULL,
    dims       INTEGER NOT NULL,
    created_at TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

-- Unique index that treats NULL as a distinct value
-- When revision IS NULL, only one row per (provider, name) is allowed
-- When revision IS NOT NULL, only one row per (provider, name, revision) is allowed
CREATE UNIQUE INDEX embedding_models_unique_key
    ON embedding_models(provider, name, COALESCE(revision, ''));


/* ------------------------------ */

CREATE TABLE embeddings_cache (
    model_id       INTEGER NOT NULL REFERENCES embedding_models(id),
    input_hash     BLOB NOT NULL,
    input_text     TEXT NOT NULL,
    embedding      BLOB NOT NULL,

    created_at     TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    last_seen_at   TEXT NOT NULL DEFAULT (CURRENT_TIMESTAMP),
    hit_count      INTEGER NOT NULL DEFAULT 0,

    PRIMARY KEY (model_id, input_hash)
);

CREATE INDEX embeddings_cache_last_seen_idx
  ON embeddings_cache(last_seen_at);

/* ------------------------------ */

CREATE TRIGGER embeddings_cache_check_dims_insert
BEFORE INSERT ON embeddings_cache
FOR EACH ROW
BEGIN
    SELECT
      CASE
        WHEN (SELECT dims FROM embedding_models WHERE id = NEW.model_id) IS NULL
          THEN RAISE(ABORT, 'Unknown model_id')
        WHEN length(NEW.embedding) != (SELECT dims FROM embedding_models WHERE id = NEW.model_id) * 4
          THEN RAISE(ABORT, 'Embedding dims mismatch (expected dims * 4 bytes)')
      END;
END;

CREATE TRIGGER embeddings_cache_check_dims_update
BEFORE UPDATE OF model_id, embedding ON embeddings_cache
FOR EACH ROW
BEGIN
    SELECT
      CASE
        WHEN (SELECT dims FROM embedding_models WHERE id = NEW.model_id) IS NULL
          THEN RAISE(ABORT, 'Unknown model_id')
        WHEN length(NEW.embedding) != (SELECT dims FROM embedding_models WHERE id = NEW.model_id) * 4
          THEN RAISE(ABORT, 'Embedding dims mismatch (expected dims * 4 bytes)')
      END;
END;

/* ------------------------------ */