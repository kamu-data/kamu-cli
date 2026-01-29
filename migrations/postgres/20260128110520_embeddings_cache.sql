/* ------------------------------ */

CREATE TABLE embedding_models (
    id              BIGSERIAL PRIMARY KEY,
    provider        TEXT NOT NULL,
    name            TEXT NOT NULL,
    revision        TEXT NULL,    
    dims            INT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Unique index that treats NULL as a distinct value
-- When revision IS NULL, only one row per (provider, name) is allowed
-- When revision IS NOT NULL, only one row per (provider, name, revision) is allowed
CREATE UNIQUE INDEX embedding_models_unique_key
    ON embedding_models(provider, name, COALESCE(revision, ''));

/* ------------------------------ */

CREATE TABLE embeddings_cache (
    model_id        BIGINT NOT NULL REFERENCES embedding_models(id),
    input_hash      BYTEA NOT NULL,
    input_text      TEXT NOT NULL,
    embedding       BYTEA NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_seen_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    hit_count       BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (model_id, input_hash)
);

CREATE INDEX embeddings_cache_last_seen_idx
  ON embeddings_cache(last_seen_at);

/* ------------------------------ */

CREATE OR REPLACE FUNCTION trg_embeddings_cache_check_dims()
RETURNS TRIGGER AS $$
DECLARE
    expected INT;
    actual   INT;
BEGIN
    SELECT dims INTO expected
    FROM embedding_models
    WHERE id = new.model_id;

    IF expected IS NULL THEN
        RAISE EXCEPTION 'Unknown model_id %', new.model_id;
    END IF;

    IF octet_length(new.embedding) <> expected * 4 THEN
        raise exception 'Embedding dims mismatch: expected % bytes, got %',
        expected * 4, octet_length(new.embedding);
    END IF;

    RETURN new;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER embeddings_cache_check_dims
BEFORE INSERT OR UPDATE OF model_id, embedding
ON embeddings_cache
FOR EACH ROW
EXECUTE FUNCTION trg_embeddings_cache_check_dims();

/* ------------------------------ */
