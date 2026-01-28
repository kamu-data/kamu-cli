// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use dill::*;
use internal_error::InternalError;
use kamu_search::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryEmbeddingsCacheRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn EmbeddingsCacheRepository)]
#[scope(Singleton)]
impl InMemoryEmbeddingsCacheRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    models: HashMap<(String, String, Option<String>), EmbeddingModelRow>,
    next_model_id: i64,
    cache: HashMap<EmbeddingCacheKey, EmbeddingBytes>,
    cache_metadata: HashMap<EmbeddingCacheKey, CacheMetadata>,
}

#[derive(Clone)]
struct CacheMetadata {
    #[allow(dead_code)]
    created_at: DateTime<Utc>,
    last_seen_at: DateTime<Utc>,
    hit_count: i64,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EmbeddingsCacheRepository for InMemoryEmbeddingsCacheRepository {
    async fn ensure_model(
        &self,
        key: &EmbeddingModelKey,
        dims: usize,
    ) -> Result<EmbeddingModelRow, EmbeddingsCacheError> {
        let mut guard = self.state.lock().unwrap();

        let lookup_key = (
            key.provider.to_string(),
            key.name.clone(),
            key.revision.map(std::string::ToString::to_string),
        );

        if let Some(existing) = guard.models.get(&lookup_key) {
            if existing.dims != dims {
                return Err(EmbeddingsCacheError::Internal(InternalError::new(format!(
                    "Embedding model dimension mismatch for {}/{}: expected {dims}",
                    key.provider, key.name
                ))));
            }
            return Ok(existing.clone());
        }

        let model_id = guard.next_model_id;
        guard.next_model_id += 1;

        let model_row = EmbeddingModelRow {
            id: model_id,
            key: EmbeddingModelKey {
                provider: key.provider,
                name: key.name.clone(),
                revision: key.revision,
            },
            dims,
            created_at: Utc::now(),
        };

        guard.models.insert(lookup_key, model_row.clone());

        Ok(model_row)
    }

    async fn get_many(
        &self,
        keys: &[EmbeddingCacheKey],
    ) -> Result<Vec<(EmbeddingCacheKey, Vec<u8>)>, EmbeddingsCacheError> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let guard = self.state.lock().unwrap();

        let result = keys
            .iter()
            .filter_map(|key| {
                guard
                    .cache
                    .get(key)
                    .map(|embedding| (key.clone(), embedding.clone()))
            })
            .collect();

        Ok(result)
    }

    async fn put_many_if_absent(
        &self,
        rows: &[(EmbeddingCacheKey, Vec<u8>)],
    ) -> Result<(), EmbeddingsCacheError> {
        if rows.is_empty() {
            return Ok(());
        }

        let mut guard = self.state.lock().unwrap();
        let now = Utc::now();

        for (key, embedding) in rows {
            if !guard.cache.contains_key(key) {
                guard.cache.insert(key.clone(), embedding.clone());
                guard.cache_metadata.insert(
                    key.clone(),
                    CacheMetadata {
                        created_at: now,
                        last_seen_at: now,
                        hit_count: 0,
                    },
                );
            }
        }

        Ok(())
    }

    async fn touch_many(
        &self,
        keys: &[EmbeddingCacheKey],
        now: DateTime<Utc>,
    ) -> Result<(), EmbeddingsCacheError> {
        if keys.is_empty() {
            return Ok(());
        }

        let mut guard = self.state.lock().unwrap();

        for key in keys {
            if let Some(metadata) = guard.cache_metadata.get_mut(key) {
                metadata.last_seen_at = now;
                metadata.hit_count += 1;
            }
        }

        Ok(())
    }

    async fn evict_older_than(
        &self,
        older_than: DateTime<Utc>,
        limit: i64,
    ) -> Result<u64, EmbeddingsCacheError> {
        let mut guard = self.state.lock().unwrap();

        // Collect keys to evict
        let mut candidates: Vec<_> = guard
            .cache_metadata
            .iter()
            .filter(|(_, metadata)| metadata.last_seen_at < older_than)
            .map(|(key, metadata)| (key.clone(), metadata.last_seen_at))
            .collect();

        // Sort by last_seen_at ascending
        candidates.sort_by_key(|(_, last_seen_at)| *last_seen_at);

        // Take up to limit
        let to_evict: Vec<_> = candidates
            .into_iter()
            .take(usize::try_from(limit).unwrap_or(usize::MAX))
            .map(|(key, _)| key)
            .collect();

        let evicted_count = to_evict.len();

        // Remove from cache and metadata
        for key in to_evict {
            guard.cache.remove(&key);
            guard.cache_metadata.remove(&key);
        }

        Ok(u64::try_from(evicted_count).unwrap())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
