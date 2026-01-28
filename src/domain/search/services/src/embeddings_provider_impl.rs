// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::Utc;
use internal_error::*;
use kamu_search::*;
use tokio::sync::OnceCell;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct EmbeddingsProviderImpl {
    embeddings_chunker: Arc<dyn EmbeddingsChunker>,
    embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
    embeddings_cache_repo: Arc<dyn EmbeddingsCacheRepository>,
    cached_model: OnceCell<EmbeddingModelRow>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn EmbeddingsProvider)]
impl EmbeddingsProviderImpl {
    fn new(
        embeddings_chunker: Arc<dyn EmbeddingsChunker>,
        embeddings_encoder: Arc<dyn EmbeddingsEncoder>,
        embeddings_cache_repo: Arc<dyn EmbeddingsCacheRepository>,
    ) -> Self {
        Self {
            embeddings_chunker,
            embeddings_encoder,
            embeddings_cache_repo,
            cached_model: OnceCell::new(),
        }
    }

    fn normalize_input_chunk(s: &str) -> String {
        let trimmed = s.trim();

        // Replace all whitespace characters (tabs, newlines, etc.) with spaces
        // and collapse consecutive whitespace to a single space.
        let mut out = String::with_capacity(trimmed.len());
        let mut prev_ws = false;
        for ch in trimmed.chars() {
            let is_ws = ch.is_whitespace();
            if is_ws {
                if !prev_ws {
                    out.push(' ');
                }
            } else {
                out.push(ch);
            }
            prev_ws = is_ws;
        }
        out
    }

    fn sha256_32(bytes: &[u8]) -> [u8; 32] {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(bytes);
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        out
    }

    fn pack_f32_le(vec: &[f32], expected_dims: usize) -> Result<Vec<u8>, InternalError> {
        if vec.len() != expected_dims {
            return Err(InternalError::new(format!(
                "Embedding dims mismatch: expected {expected_dims}, got {}",
                vec.len()
            )));
        }
        let mut out = Vec::with_capacity(vec.len() * 4);
        for &v in vec {
            out.extend_from_slice(&v.to_le_bytes());
        }
        Ok(out)
    }

    fn unpack_f32_le(bytes: &[u8], expected_dims: usize) -> Result<Vec<f32>, InternalError> {
        let expected_bytes = expected_dims * 4;
        if bytes.len() != expected_bytes {
            return Err(InternalError::new(format!(
                "Embedding bytes mismatch: expected {} bytes, got {}",
                expected_bytes,
                bytes.len()
            )));
        }
        let mut out = Vec::with_capacity(expected_dims);
        for chunk in bytes.chunks_exact(4) {
            out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Ok(out)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl EmbeddingsProvider for EmbeddingsProviderImpl {
    async fn provide_content_embeddings(
        &self,
        content: Vec<String>,
    ) -> Result<Vec<Vec<f32>>, InternalError> {
        // 1) Ensure model exists (so we have model_id).
        let model = self
            .cached_model
            .get_or_try_init(|| async {
                self.embeddings_cache_repo
                    .ensure_model(
                        &self.embeddings_encoder.model_key(),
                        self.embeddings_encoder.dimensions(),
                    )
                    .await
                    .int_err()
            })
            .await?;

        // 2) Split parts into chunks
        let chunks = self.embeddings_chunker.chunk(content).await?;
        if chunks.is_empty() {
            return Ok(vec![]);
        }

        // 3) Normalize chunks
        let normalized_chunks: Vec<String> = chunks
            .iter()
            .map(|s| Self::normalize_input_chunk(s))
            .collect();

        // 4) Check cache for existing embeddings
        let cache_keys: Vec<EmbeddingCacheKey> = normalized_chunks
            .iter()
            .map(|s| EmbeddingCacheKey {
                model_id: model.id,
                input_hash: Self::sha256_32(s.as_bytes()),
                input_text: s.clone(),
            })
            .collect();

        let hits = self
            .embeddings_cache_repo
            .get_many(&cache_keys)
            .await
            .int_err()?;

        // Convert hits vec to lookup map
        let mut hit_map = HashMap::<EmbeddingCacheKey, Vec<u8>>::new();
        for (k, v) in hits {
            hit_map.insert(k, v);
        }

        // 5) Identify misses in input order
        let mut miss_positions = Vec::<usize>::new();
        let mut miss_texts = Vec::<String>::new();
        for (idx, k) in cache_keys.iter().enumerate() {
            if !hit_map.contains_key(k) {
                miss_positions.push(idx);
                miss_texts.push(normalized_chunks[idx].clone());
            }
        }

        // 6) Encode misses via encoder (chunk by max_batch)
        if !miss_texts.is_empty() {
            let mut newly_computed: Vec<(EmbeddingCacheKey, Vec<u8>)> = Vec::new();

            let new_vecs = self.embeddings_encoder.encode(miss_texts).await?;
            if !new_vecs.is_empty() {
                assert_eq!(new_vecs.len(), miss_positions.len());

                for (i, vec) in new_vecs.into_iter().enumerate() {
                    let original_position = miss_positions[i];
                    let k = cache_keys[original_position].clone();

                    let bytes = Self::pack_f32_le(&vec, model.dims)?;
                    newly_computed.push((k.clone(), bytes.clone()));
                    hit_map.insert(k, bytes);
                }

                // 7) Persist computed embeddings (idempotent)
                self.embeddings_cache_repo
                    .put_many_if_absent(&newly_computed)
                    .await
                    .int_err()?;
            }
        }

        // Handle empty case
        if hit_map.is_empty() {
            return Ok(vec![]);
        }

        // 8) Touch stats
        self.embeddings_cache_repo
            .touch_many(&cache_keys, Utc::now())
            .await
            .int_err()?;

        // 9) Produce output in original order
        let mut out = Vec::with_capacity(chunks.len());
        for k in &cache_keys {
            let bytes = hit_map
                .get(k)
                .ok_or_else(|| InternalError::new("Cache invariant failed: missing embedding"))?;
            out.push(Self::unpack_f32_le(bytes, model.dims)?);
        }

        Ok(out)
    }

    async fn provide_prompt_embeddings(&self, prompt: String) -> Result<Vec<f32>, InternalError> {
        // 1) Ensure model exists (so we have model_id).
        let model = self
            .cached_model
            .get_or_try_init(|| async {
                self.embeddings_cache_repo
                    .ensure_model(
                        &self.embeddings_encoder.model_key(),
                        self.embeddings_encoder.dimensions(),
                    )
                    .await
                    .int_err()
            })
            .await?;

        // 2) Normalize prompt
        let normalized_prompt = Self::normalize_input_chunk(&prompt);

        // 3) Create cache key
        let cache_key = EmbeddingCacheKey {
            model_id: model.id,
            input_hash: Self::sha256_32(normalized_prompt.as_bytes()),
            input_text: normalized_prompt.clone(),
        };

        // 4) Check cache
        let hits = self
            .embeddings_cache_repo
            .get_many(std::slice::from_ref(&cache_key))
            .await
            .int_err()?;

        let embedding_bytes = if let Some((_, bytes)) = hits.into_iter().next() {
            // Cache hit
            bytes
        } else {
            // Cache miss - encode
            let prompt_vec = self
                .embeddings_encoder
                .encode(vec![normalized_prompt])
                .await
                .int_err()?
                .into_iter()
                .next()
                .unwrap();

            let bytes = Self::pack_f32_le(&prompt_vec, model.dims)?;

            // 5) Persist to cache (idempotent)
            self.embeddings_cache_repo
                .put_many_if_absent(&[(cache_key.clone(), bytes.clone())])
                .await
                .int_err()?;

            bytes
        };

        // 6) Touch stats
        self.embeddings_cache_repo
            .touch_many(&[cache_key], Utc::now())
            .await
            .int_err()?;

        // 7) Unpack and return
        Self::unpack_f32_le(&embedding_bytes, model.dims)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
