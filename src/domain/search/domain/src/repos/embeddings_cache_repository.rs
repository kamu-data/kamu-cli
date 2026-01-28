// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;

use crate::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait EmbeddingsCacheRepository: Send + Sync {
    /// Ensures that the embedding model is registered in the cache
    async fn ensure_model(
        &self,
        key: &EmbeddingModelKey,
        dims: usize,
    ) -> Result<EmbeddingModelRow, EmbeddingsCacheError>;

    /// Returns a map-like vec: only hits are returned.
    async fn get_many(
        &self,
        keys: &[EmbeddingCacheKey],
    ) -> Result<Vec<(EmbeddingCacheKey, Vec<u8>)>, EmbeddingsCacheError>;

    /// Inserts only missing rows. Must be idempotent.
    async fn put_many_if_absent(
        &self,
        rows: &[(EmbeddingCacheKey, Vec<u8>)],
    ) -> Result<(), EmbeddingsCacheError>;

    /// Stats bump for keys we used
    async fn touch_many(
        &self,
        keys: &[EmbeddingCacheKey],
        now: DateTime<Utc>,
    ) -> Result<(), EmbeddingsCacheError>;

    /// Cache eviction logic
    async fn evict_older_than(
        &self,
        older_than: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64, EmbeddingsCacheError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum EmbeddingsCacheError {
    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
