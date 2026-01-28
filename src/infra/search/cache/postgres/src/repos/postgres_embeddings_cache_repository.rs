// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::TransactionRefT;
use dill::{component, interface};
use internal_error::{InternalError, ResultIntoInternal};
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn EmbeddingsCacheRepository)]
pub struct PostgresEmbeddingsCacheRepository {
    transaction: TransactionRefT<sqlx::Postgres>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EmbeddingsCacheRepository for PostgresEmbeddingsCacheRepository {
    /// Ensures that the embedding model is registered in the cache
    async fn ensure_model(
        &self,
        key: &EmbeddingModelKey,
        dims: usize,
    ) -> Result<EmbeddingModelRow, EmbeddingsCacheError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let maybe_record = sqlx::query!(
            r#"
            INSERT INTO embedding_models (provider, name, revision, dims)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (provider, name, COALESCE(revision, ''))
                DO UPDATE
                    SET dims = embedding_models.dims
                    WHERE embedding_models.dims = EXCLUDED.dims
                RETURNING id, provider, name, revision, dims, created_at;
            "#,
            key.provider,
            key.name,
            key.revision,
            i32::try_from(dims).unwrap(),
        )
        .fetch_optional(connection_mut)
        .await
        .int_err()?;

        let record = maybe_record.ok_or_else(|| {
            EmbeddingsCacheError::Internal(InternalError::new(format!(
                "Embedding model dimension mismatch for {}/{}: expected {dims}",
                key.provider, key.name
            )))
        })?;

        Ok(EmbeddingModelRow {
            id: record.id,
            key: EmbeddingModelKey {
                provider: key.provider,
                name: record.name,
                revision: key.revision,
            },
            dims: usize::try_from(record.dims).unwrap(),
            created_at: record.created_at,
        })
    }

    /// Returns a map-like vec: only hits are returned.
    async fn get_many(
        &self,
        keys: &[EmbeddingCacheKey],
    ) -> Result<Vec<(EmbeddingCacheKey, Vec<u8>)>, EmbeddingsCacheError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        // Build query with UNNEST for efficient batch lookup
        let model_ids: Vec<i64> = keys.iter().map(|k| k.model_id).collect();
        let input_hashes: Vec<Vec<u8>> = keys.iter().map(|k| k.input_hash.to_vec()).collect();

        let records = sqlx::query!(
            r#"
            SELECT model_id, input_hash, input_text, embedding
                FROM embeddings_cache
            WHERE (model_id, input_hash) IN (
                SELECT * FROM UNNEST($1::BIGINT[], $2::BYTEA[])
            )
            "#,
            &model_ids[..],
            &input_hashes[..],
        )
        .fetch_all(connection_mut)
        .await
        .int_err()?;

        let result = records
            .into_iter()
            .map(|record| {
                let mut input_hash = [0u8; 32];
                input_hash.copy_from_slice(&record.input_hash);
                let key = EmbeddingCacheKey {
                    model_id: record.model_id,
                    input_hash,
                    input_text: record.input_text,
                };
                (key, record.embedding)
            })
            .collect();

        Ok(result)
    }

    /// Inserts only missing rows. Must be idempotent.
    async fn put_many_if_absent(
        &self,
        rows: &[(EmbeddingCacheKey, Vec<u8>)],
    ) -> Result<(), EmbeddingsCacheError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let model_ids: Vec<i64> = rows.iter().map(|r| r.0.model_id).collect();
        let input_hashes: Vec<Vec<u8>> = rows.iter().map(|r| r.0.input_hash.to_vec()).collect();
        let input_texts: Vec<String> = rows.iter().map(|r| r.0.input_text.clone()).collect();
        let embeddings: Vec<Vec<u8>> = rows.iter().map(|r| r.1.clone()).collect();

        sqlx::query!(
            r#"
            INSERT INTO embeddings_cache (model_id, input_hash, input_text, embedding)
                SELECT * FROM UNNEST($1::BIGINT[], $2::BYTEA[], $3::TEXT[], $4::BYTEA[])
                ON CONFLICT (model_id, input_hash) DO NOTHING
            "#,
            &model_ids[..],
            &input_hashes[..],
            &input_texts[..],
            &embeddings[..],
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    /// Stats bump for keys we used
    async fn touch_many(
        &self,
        keys: &[EmbeddingCacheKey],
        now: DateTime<Utc>,
    ) -> Result<(), EmbeddingsCacheError> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let model_ids: Vec<i64> = keys.iter().map(|k| k.model_id).collect();
        let input_hashes: Vec<Vec<u8>> = keys.iter().map(|k| k.input_hash.to_vec()).collect();

        sqlx::query!(
            r#"
            UPDATE embeddings_cache
                SET
                    last_seen_at = $1, hit_count = hit_count + 1
                WHERE (model_id, input_hash) IN (
                    SELECT * FROM UNNEST($2::BIGINT[], $3::BYTEA[])
                )
            "#,
            now,
            &model_ids[..],
            &input_hashes[..],
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(())
    }

    /// Cache eviction logic
    async fn evict_older_than(
        &self,
        older_than: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64, EmbeddingsCacheError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let result = sqlx::query!(
            r#"
            DELETE FROM embeddings_cache
                WHERE (model_id, input_hash) IN (
                    SELECT model_id, input_hash
                    FROM embeddings_cache
                    WHERE last_seen_at < $1
                    ORDER BY last_seen_at ASC
                    LIMIT $2
                )
            "#,
            older_than,
            limit,
        )
        .execute(connection_mut)
        .await
        .int_err()?;

        Ok(result.rows_affected())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
