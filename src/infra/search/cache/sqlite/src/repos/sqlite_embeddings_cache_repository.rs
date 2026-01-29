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
pub struct SqliteEmbeddingsCacheRepository {
    transaction: TransactionRefT<sqlx::Sqlite>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl EmbeddingsCacheRepository for SqliteEmbeddingsCacheRepository {
    #[tracing::instrument(
        level = "debug",
        name = SqliteEmbeddingsCacheRepository_ensure_model,
        skip_all,
        fields(key = ?key, dims = %dims)
    )]
    async fn ensure_model(
        &self,
        key: &EmbeddingModelKey,
        dims: usize,
    ) -> Result<EmbeddingModelRow, EmbeddingsCacheError> {
        let mut tr = self.transaction.lock().await;
        let connection_mut = tr.connection_mut().await?;

        let dims_i64 = i64::try_from(dims).unwrap();

        let maybe_record = sqlx::query!(
            r#"
            INSERT INTO embedding_models (provider, name, revision, dims)
                VALUES ($1, $2, $3, $4)
            ON CONFLICT (provider, name, COALESCE(revision, ''))
            DO UPDATE
                SET dims = dims
                WHERE dims = excluded.dims
            RETURNING id, provider, name, revision, dims, created_at as "created_at: DateTime<Utc>";
            "#,
            key.provider,
            key.name,
            key.revision,
            dims_i64,
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

    #[tracing::instrument(
        level = "debug",
        name = SqliteEmbeddingsCacheRepository_retrieve_embeddings_batch,
        skip_all,
        fields(keys_count = keys.len())
    )]
    async fn retrieve_embeddings_batch(
        &self,
        keys: &[EmbeddingCacheKey],
    ) -> Result<Vec<(EmbeddingCacheKey, Vec<u8>)>, EmbeddingsCacheError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let mut tr = self.transaction.lock().await;

        let mut result = Vec::new();

        // SQLite doesn't support UNNEST, so we query in a loop
        for key in keys {
            let connection_mut = tr.connection_mut().await?;
            let input_hash_slice = &key.input_hash[..];

            let maybe_record = sqlx::query!(
                r#"
                SELECT model_id as "model_id: i64", input_hash, input_text, embedding
                    FROM embeddings_cache
                    WHERE model_id = $1 AND input_hash = $2
                "#,
                key.model_id,
                input_hash_slice,
            )
            .fetch_optional(connection_mut)
            .await
            .int_err()?;

            if let Some(record) = maybe_record {
                let mut input_hash = [0u8; 32];
                input_hash.copy_from_slice(&record.input_hash);
                let cache_key = EmbeddingCacheKey {
                    model_id: record.model_id,
                    input_hash,
                    input_text: record.input_text,
                };
                result.push((cache_key, record.embedding));
            }
        }

        Ok(result)
    }

    #[tracing::instrument(
        level = "debug",
        name = SqliteEmbeddingsCacheRepository_bulk_upsert_embeddings,
        skip_all,
        fields(rows_count = rows.len())
    )]
    async fn bulk_upsert_embeddings(
        &self,
        rows: &[(EmbeddingCacheKey, Vec<u8>)],
    ) -> Result<(), EmbeddingsCacheError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;

        // SQLite doesn't support multi-row INSERT from arrays,
        // so we insert one by one
        for (key, embedding) in rows {
            let connection_mut = tr.connection_mut().await?;
            let input_hash_slice = &key.input_hash[..];

            sqlx::query!(
                r#"
                INSERT INTO embeddings_cache (model_id, input_hash, input_text, embedding)
                    VALUES ($1, $2, $3, $4)
                    ON CONFLICT (model_id, input_hash) DO NOTHING
                "#,
                key.model_id,
                input_hash_slice,
                key.input_text,
                embedding,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        name = SqliteEmbeddingsCacheRepository_touch_embeddings,
        skip_all,
        fields(keys_count = keys.len())
    )]
    async fn touch_embeddings(
        &self,
        keys: &[EmbeddingCacheKey],
        now: DateTime<Utc>,
    ) -> Result<(), EmbeddingsCacheError> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut tr = self.transaction.lock().await;

        // SQLite doesn't support updating multiple rows with UNNEST,
        // so we update one by one
        for key in keys {
            let connection_mut = tr.connection_mut().await?;
            let input_hash_slice = &key.input_hash[..];

            sqlx::query!(
                r#"
                UPDATE embeddings_cache
                    SET last_seen_at = $1, hit_count = hit_count + 1
                    WHERE model_id = $2 AND input_hash = $3
                "#,
                now,
                key.model_id,
                input_hash_slice,
            )
            .execute(connection_mut)
            .await
            .int_err()?;
        }

        Ok(())
    }

    #[tracing::instrument(
        level = "debug",
        name = SqliteEmbeddingsCacheRepository_evict_older_than,
        skip_all,
        fields(older_than, limit)
    )]
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
                WHERE rowid IN (
                    SELECT rowid
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
