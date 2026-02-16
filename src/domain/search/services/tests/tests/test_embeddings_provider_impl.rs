// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use dill::*;
use internal_error::InternalError;
use kamu_search::*;
use kamu_search_cache_inmem::InMemoryEmbeddingsCacheRepository;
use kamu_search_services::*;
use time_source::SystemTimeSourceStub;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Encoder Cache Optimization Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_encoder_cache_miss_on_first_call() {
    let harness = EmbeddingsProviderHarness::new();

    let content = vec![
        "First chunk".to_string(),
        "Second chunk".to_string(),
        "Third chunk".to_string(),
    ];

    let embeddings = harness
        .provider()
        .provide_content_embeddings(content.clone())
        .await
        .unwrap();

    // Should get one embedding per chunk
    assert_eq!(embeddings.len(), 3);

    // Encoder should be called once for all chunks (cache miss)
    assert_eq!(
        harness.encoder().encode_call_count(),
        1,
        "Encoder should be called once on cache miss"
    );

    // Verify all chunks were encoded
    let encoded_chunks = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(
        encoded_chunks, content,
        "All chunks should be encoded on first call"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_encoder_cache_hit_no_encoding() {
    let harness = EmbeddingsProviderHarness::new();

    let content = vec!["First chunk".to_string(), "Second chunk".to_string()];

    // First call - cache miss
    let embeddings1 = harness
        .provider()
        .provide_content_embeddings(content.clone())
        .await
        .unwrap();

    assert_eq!(harness.encoder().encode_call_count(), 1);
    let encoded_after_first = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(
        encoded_after_first, content,
        "First call should encode all chunks"
    );

    // Second call with same content - should hit cache
    let embeddings2 = harness
        .provider()
        .provide_content_embeddings(content.clone())
        .await
        .unwrap();

    // Encoder should NOT be called again
    assert_eq!(
        harness.encoder().encode_call_count(),
        1,
        "Encoder should not be called on cache hit"
    );

    // No additional chunks should be encoded
    let encoded_after_second = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(
        encoded_after_second, content,
        "No new chunks should be encoded on cache hit"
    );

    // Results should be identical
    assert_eq!(embeddings1, embeddings2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_encoder_partial_cache_hit_only_encodes_new() {
    let harness = EmbeddingsProviderHarness::new();

    // First call with 2 chunks
    let content1 = vec!["Cached chunk 1".to_string(), "Cached chunk 2".to_string()];

    harness
        .provider()
        .provide_content_embeddings(content1.clone())
        .await
        .unwrap();

    assert_eq!(harness.encoder().encode_call_count(), 1);
    let encoded_batches = harness.encoder().get_encoded_chunks();
    assert_eq!(encoded_batches.len(), 1);
    assert_eq!(
        encoded_batches[0], content1,
        "First call should encode all chunks"
    );

    // Second call with 1 cached chunk + 2 new chunks
    let content2 = vec![
        "Cached chunk 1".to_string(), // This should come from cache
        "New chunk 1".to_string(),    // This should be encoded
        "New chunk 2".to_string(),    // This should be encoded
    ];

    let embeddings = harness
        .provider()
        .provide_content_embeddings(content2)
        .await
        .unwrap();

    assert_eq!(embeddings.len(), 3);

    // Encoder should be called again, but only for the 2 new chunks
    assert_eq!(
        harness.encoder().encode_call_count(),
        2,
        "Encoder should be called once more for new chunks only"
    );

    // Verify only new chunks were encoded in second call
    let encoded_batches = harness.encoder().get_encoded_chunks();
    assert_eq!(encoded_batches.len(), 2, "Should have 2 encoding batches");
    assert_eq!(
        encoded_batches[1],
        vec!["New chunk 1".to_string(), "New chunk 2".to_string()],
        "Second call should only encode new chunks"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_prompt_embeddings_cache_behavior() {
    let harness = EmbeddingsProviderHarness::new();

    let prompt = "Search for datasets about climate change".to_string();

    // First call - cache miss
    let embedding1 = harness
        .provider()
        .provide_prompt_embeddings(prompt.clone())
        .await
        .unwrap();

    assert_eq!(harness.encoder().encode_call_count(), 1);
    let encoded_chunks = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(
        encoded_chunks,
        vec![prompt.clone()],
        "Prompt should be encoded on first call"
    );

    // Second call with same prompt - cache hit
    let embedding2 = harness
        .provider()
        .provide_prompt_embeddings(prompt.clone())
        .await
        .unwrap();

    // Encoder should NOT be called again
    assert_eq!(
        harness.encoder().encode_call_count(),
        1,
        "Encoder should not be called on cache hit for prompt"
    );

    // No additional chunks should be encoded
    let encoded_after_second = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(
        encoded_after_second,
        vec![prompt],
        "No additional chunks should be encoded on cache hit"
    );

    // Results should be identical
    assert_eq!(embedding1, embedding2);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Normalization Boundary Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_normalization_whitespace_variations_same_cache() {
    let harness = EmbeddingsProviderHarness::new();

    // Different whitespace representations of the same content
    let variations = [
        "Hello world".to_string(),
        "  Hello   world  ".to_string(),     // Extra spaces
        "Hello\tworld".to_string(),          // Tab instead of space
        "Hello\nworld".to_string(),          // Newline instead of space
        "  Hello \n\t  world  ".to_string(), // Mixed whitespace
    ];

    // All variations should produce same normalized chunk and hit cache
    for (i, variation) in variations.iter().enumerate() {
        harness
            .provider()
            .provide_content_embeddings(vec![variation.clone()])
            .await
            .unwrap();

        // Only first variation should trigger encoder
        assert_eq!(
            harness.encoder().encode_call_count(),
            1,
            "Variation {i} should hit cache, encoder should only be called once"
        );
    }

    // Verify only one unique chunk was encoded
    let encoded_chunks = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(encoded_chunks.len(), 1);
    assert_eq!(encoded_chunks[0], "Hello world"); // Normalized form
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_normalization_case_sensitive() {
    let harness = EmbeddingsProviderHarness::new();

    // Case differences should create different cache entries
    let variations = vec!["Hello World".to_string(), "hello world".to_string()];

    for variation in &variations {
        harness
            .provider()
            .provide_content_embeddings(vec![variation.clone()])
            .await
            .unwrap();
    }

    // Both should trigger encoder (case sensitive)
    assert_eq!(
        harness.encoder().encode_call_count(),
        2,
        "Case variations should create separate cache entries"
    );

    let encoded_chunks = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(encoded_chunks.len(), 2);
    assert_eq!(encoded_chunks[0], "Hello World");
    assert_eq!(encoded_chunks[1], "hello world");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_normalization_punctuation_sensitive() {
    let harness = EmbeddingsProviderHarness::new();

    // Punctuation differences should create different cache entries
    let variations = vec![
        "Hello, world!".to_string(),
        "Hello world".to_string(),
        "Hello. World.".to_string(),
    ];

    for variation in &variations {
        harness
            .provider()
            .provide_content_embeddings(vec![variation.clone()])
            .await
            .unwrap();
    }

    // All should trigger encoder (punctuation sensitive)
    assert_eq!(
        harness.encoder().encode_call_count(),
        3,
        "Punctuation variations should create separate cache entries"
    );

    let encoded_chunks = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(encoded_chunks.len(), 3);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_normalization_empty_and_whitespace_only() {
    let harness = EmbeddingsProviderHarness::new();

    // Empty and whitespace-only strings normalize to empty and get filtered out
    let variations = vec![String::new(), "   ".to_string(), "\t\n  ".to_string()];

    for variation in &variations {
        let result = harness
            .provider()
            .provide_content_embeddings(vec![variation.clone()])
            .await
            .unwrap();

        // Should return empty result after filtering out empty normalized chunks
        assert_eq!(
            result.len(),
            0,
            "Empty/whitespace-only inputs should produce no embeddings"
        );
    }

    // Encoder should never be called for empty inputs
    assert_eq!(
        harness.encoder().encode_call_count(),
        0,
        "Encoder should not be called for whitespace-only inputs"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_normalization_filters_empty_keeps_non_empty() {
    let harness = EmbeddingsProviderHarness::new();

    // Mix of empty and non-empty chunks
    let content = vec![
        "   ".to_string(),   // Normalizes to empty - filtered out
        "Hello".to_string(), // Kept
        "\t\n".to_string(),  // Normalizes to empty - filtered out
        "World".to_string(), // Kept
        "  ".to_string(),    // Normalizes to empty - filtered out
    ];

    let result = harness
        .provider()
        .provide_content_embeddings(content)
        .await
        .unwrap();

    // Should only get embeddings for non-empty chunks
    assert_eq!(
        result.len(),
        2,
        "Should return 2 embeddings for non-empty chunks"
    );

    // Encoder should be called once with only the non-empty chunks
    assert_eq!(harness.encoder().encode_call_count(), 1);

    let encoded_chunks = harness.encoder().get_all_encoded_chunks_flat();
    assert_eq!(encoded_chunks.len(), 2);
    assert_eq!(encoded_chunks[0], "Hello");
    assert_eq!(encoded_chunks[1], "World");
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Touch Logic Tests
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_touch_updates_last_use_time() {
    use chrono::{TimeZone, Utc};

    let initial_time = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
    let time_source = SystemTimeSourceStub::new_set(initial_time);
    let harness = EmbeddingsProviderHarness::new_with_time(time_source);

    let content = vec!["Test chunk".to_string()];

    // First call at T0 - cache miss
    harness
        .provider()
        .provide_content_embeddings(content.clone())
        .await
        .unwrap();

    // Advance time by 1 hour
    let later_time = initial_time + chrono::Duration::hours(1);
    harness.time_source().set(later_time);

    // Second call at T0+1h - cache hit, should update touch time
    harness
        .provider()
        .provide_content_embeddings(content)
        .await
        .unwrap();

    // Encoder should only be called once
    assert_eq!(
        harness.encoder().encode_call_count(),
        1,
        "Should hit cache on second call"
    );

    // Evict embeddings older than T0+30min
    let eviction_cutoff = initial_time + chrono::Duration::minutes(30);
    harness
        .cache_repo()
        .evict_older_than(eviction_cutoff, 1000)
        .await
        .unwrap();

    // Try to retrieve again - should still be in cache because touch time was
    // updated
    let content_again = vec!["Test chunk".to_string()];
    harness
        .provider()
        .provide_content_embeddings(content_again)
        .await
        .unwrap();

    // Should still hit cache (not evicted)
    assert_eq!(
        harness.encoder().encode_call_count(),
        1,
        "Should still hit cache after eviction because touch time was updated"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_untouched_embeddings_get_evicted() {
    use chrono::{TimeZone, Utc};

    let initial_time = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
    let time_source = SystemTimeSourceStub::new_set(initial_time);
    let harness = EmbeddingsProviderHarness::new_with_time(time_source);

    // Create two different chunks
    let old_chunk = vec!["Old chunk".to_string()];
    let new_chunk = vec!["New chunk".to_string()];

    // Store old chunk at T0
    harness
        .provider()
        .provide_content_embeddings(old_chunk.clone())
        .await
        .unwrap();

    // Advance time by 2 hours
    let later_time = initial_time + chrono::Duration::hours(2);
    harness.time_source().set(later_time);

    // Store new chunk at T0+2h (and touch old chunk's time if accessed, but we
    // won't access it)
    harness
        .provider()
        .provide_content_embeddings(new_chunk.clone())
        .await
        .unwrap();

    // Both should have been encoded once
    assert_eq!(harness.encoder().encode_call_count(), 2);

    // Evict embeddings older than T0+1h
    let eviction_cutoff = initial_time + chrono::Duration::hours(1);
    harness
        .cache_repo()
        .evict_older_than(eviction_cutoff, 1000)
        .await
        .unwrap();

    // Old chunk should be evicted, new chunk should remain

    // Try to retrieve old chunk - should trigger encoding
    harness
        .provider()
        .provide_content_embeddings(old_chunk)
        .await
        .unwrap();

    assert_eq!(
        harness.encoder().encode_call_count(),
        3,
        "Old chunk should be re-encoded after eviction"
    );

    // New chunk should still be in cache
    harness
        .provider()
        .provide_content_embeddings(new_chunk)
        .await
        .unwrap();

    assert_eq!(
        harness.encoder().encode_call_count(),
        3,
        "New chunk should still be in cache"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_repeated_access_prevents_eviction() {
    use chrono::{TimeZone, Utc};

    let initial_time = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();
    let time_source = SystemTimeSourceStub::new_set(initial_time);
    let harness = EmbeddingsProviderHarness::new_with_time(time_source);

    let content = vec!["Frequently accessed chunk".to_string()];

    // Initial store at T0
    harness
        .provider()
        .provide_content_embeddings(content.clone())
        .await
        .unwrap();

    // Access multiple times with time advancing
    for hour in 1..=5 {
        let time = initial_time + chrono::Duration::hours(hour);
        harness.time_source().set(time);

        harness
            .provider()
            .provide_content_embeddings(content.clone())
            .await
            .unwrap();
    }

    // Only encoded once - all subsequent were cache hits
    assert_eq!(harness.encoder().encode_call_count(), 1);

    // Evict everything older than T0+4h
    let eviction_cutoff = initial_time + chrono::Duration::hours(4);
    harness
        .cache_repo()
        .evict_older_than(eviction_cutoff, 1000)
        .await
        .unwrap();

    // Should still be in cache (last touched at T0+5h)
    harness
        .provider()
        .provide_content_embeddings(content)
        .await
        .unwrap();

    assert_eq!(
        harness.encoder().encode_call_count(),
        1,
        "Frequently accessed chunk should survive eviction"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct EmbeddingsProviderHarness {
    provider: Arc<dyn EmbeddingsProvider>,
    encoder: Arc<TestEmbeddingsEncoder>,
    cache_repo: Arc<dyn EmbeddingsCacheRepository>,
    time_source: Arc<SystemTimeSourceStub>,
}

impl EmbeddingsProviderHarness {
    fn new() -> Self {
        Self::new_with_time(SystemTimeSourceStub::new())
    }

    fn new_with_time(time_source: SystemTimeSourceStub) -> Self {
        let catalog = {
            let mut b = CatalogBuilder::new();

            // Add cache repository (in-memory)
            b.add::<InMemoryEmbeddingsCacheRepository>();

            // Use real chunker with simple config
            // Keep sections separate so each content string becomes a chunk
            let chunker_config = EmbeddingsChunkerConfigSimple {
                split_sections: true,
                split_paragraphs: false,
            };
            b.add_value(chunker_config);
            b.add::<EmbeddingsChunkerSimple>();

            // Add test encoder
            b.add::<TestEmbeddingsEncoder>();

            // Add time source
            b.add_value(time_source);
            b.bind::<dyn time_source::SystemTimeSource, SystemTimeSourceStub>();

            // Add the provider under test
            b.add::<EmbeddingsProviderImpl>();

            b.build()
        };

        let provider = catalog.get_one().unwrap();
        let encoder = catalog.get_one().unwrap();
        let cache_repo = catalog.get_one().unwrap();
        let time_source = catalog.get_one().unwrap();

        Self {
            provider,
            encoder,
            cache_repo,
            time_source,
        }
    }

    fn provider(&self) -> &dyn EmbeddingsProvider {
        self.provider.as_ref()
    }

    fn encoder(&self) -> &TestEmbeddingsEncoder {
        self.encoder.as_ref()
    }

    fn cache_repo(&self) -> &dyn EmbeddingsCacheRepository {
        self.cache_repo.as_ref()
    }

    fn time_source(&self) -> &SystemTimeSourceStub {
        self.time_source.as_ref()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Test Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

static ENCODER_CALL_COUNT: AtomicUsize = AtomicUsize::new(0);
static ENCODED_CHUNKS: Mutex<Vec<Vec<String>>> = Mutex::new(Vec::new());

struct TestEmbeddingsEncoder;

#[dill::component(pub)]
#[dill::interface(dyn EmbeddingsEncoder)]
impl TestEmbeddingsEncoder {
    fn new() -> Self {
        // Reset counter and chunks at creation
        ENCODER_CALL_COUNT.store(0, Ordering::SeqCst);
        ENCODED_CHUNKS.lock().unwrap().clear();
        Self
    }

    fn encode_call_count(&self) -> usize {
        ENCODER_CALL_COUNT.load(Ordering::SeqCst)
    }

    fn get_encoded_chunks(&self) -> Vec<Vec<String>> {
        ENCODED_CHUNKS.lock().unwrap().clone()
    }

    fn get_all_encoded_chunks_flat(&self) -> Vec<String> {
        ENCODED_CHUNKS
            .lock()
            .unwrap()
            .iter()
            .flat_map(Clone::clone)
            .collect()
    }
}

#[async_trait::async_trait]
impl EmbeddingsEncoder for TestEmbeddingsEncoder {
    fn model_key(&self) -> EmbeddingModelKey {
        EmbeddingModelKey {
            provider: "test-provider",
            name: "test-model".to_string(),
            revision: Some("v1.0"),
        }
    }

    fn dimensions(&self) -> usize {
        128 // Smaller than real models for testing
    }

    #[allow(clippy::cast_precision_loss)]
    async fn encode(&self, input: Vec<String>) -> Result<Vec<Vec<f32>>, InternalError> {
        ENCODER_CALL_COUNT.fetch_add(1, Ordering::SeqCst);

        // Track which chunks were encoded
        ENCODED_CHUNKS.lock().unwrap().push(input.clone());

        // Generate deterministic embeddings based on input text
        // This ensures same input always produces same output
        let mut result = Vec::new();

        for text in input {
            let mut embedding = vec![0.0f32; self.dimensions()];

            // Simple deterministic encoding: use text hash to seed values
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher = DefaultHasher::new();
            text.hash(&mut hasher);
            let hash = hasher.finish();

            // Fill embedding with deterministic values based on hash
            for (i, val) in embedding.iter_mut().enumerate() {
                let seed = hash.wrapping_add(i as u64);
                *val = ((seed % 1000) as f32) / 1000.0; // Values in [0, 1)
            }

            result.push(embedding);
        }

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
