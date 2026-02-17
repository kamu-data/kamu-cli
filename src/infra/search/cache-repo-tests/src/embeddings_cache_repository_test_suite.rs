// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{TimeDelta, Utc};
use dill::Catalog;
use kamu_search::*;
use sha2::{Digest, Sha256};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ensure_model(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    let model_key = EmbeddingModelKey {
        provider: "test-provider",
        name: "test-model".to_string(),
        revision: Some("v1.0"),
    };

    let result = repo.ensure_model(&model_key, 384).await;

    assert!(
        result.is_ok(),
        "ensure_model should succeed, but got error: {result:?}"
    );

    let model_row = result.unwrap();
    assert_eq!(model_row.key, model_key);
    assert_eq!(model_row.dims, 384);

    // Register the same model again - should get the same ID
    let model_row2 = repo.ensure_model(&model_key, 384).await.unwrap();
    assert_eq!(
        model_row.id, model_row2.id,
        "Registering the same model twice should return the same ID"
    );
    assert_eq!(model_row2.key, model_key);
    assert_eq!(model_row2.dims, 384);

    // Register a different model - should get a new ID
    let different_model_key = EmbeddingModelKey {
        provider: "different-provider",
        name: "different-model".to_string(),
        revision: Some("v2.0"),
    };
    let different_model_row = repo.ensure_model(&different_model_key, 512).await.unwrap();
    assert_ne!(
        model_row.id, different_model_row.id,
        "Different models should get different IDs"
    );
    assert_eq!(different_model_row.key, different_model_key);
    assert_eq!(different_model_row.dims, 512);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_retrieve_embeddings_batch_empty(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    // Ensure model first
    let model_key = EmbeddingModelKey {
        provider: "test-provider",
        name: "text-embedding-3-small".to_string(),
        revision: Some("v1"),
    };
    let model_row = repo.ensure_model(&model_key, 1536).await.unwrap();

    // Try to retrieve embeddings that don't exist yet
    let keys = vec![
        EmbeddingCacheKey {
            model_id: model_row.id,
            input_hash: hash_input("hello world"),
            input_text: "hello world".to_string(),
        },
        EmbeddingCacheKey {
            model_id: model_row.id,
            input_hash: hash_input("foo bar"),
            input_text: "foo bar".to_string(),
        },
    ];

    let result = repo.retrieve_embeddings_batch(&keys).await.unwrap();

    // Should return empty - no hits
    assert!(
        result.is_empty(),
        "Expected no hits for non-existent embeddings, but got {result:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_bulk_upsert_and_retrieve(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    // Ensure model
    let model_key = EmbeddingModelKey {
        provider: "test-provider",
        name: "text-embedding-3-small".to_string(),
        revision: Some("v1"),
    };
    let model_row = repo.ensure_model(&model_key, 1536).await.unwrap();

    // Create some test embeddings
    let key1 = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("hello world"),
        input_text: "hello world".to_string(),
    };
    let embedding1 = create_mock_embedding(1536);

    let key2 = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("goodbye moon"),
        input_text: "goodbye moon".to_string(),
    };
    let embedding2 = create_mock_embedding(1536);

    // Upsert the embeddings
    let rows = vec![
        (key1.clone(), embedding1.clone()),
        (key2.clone(), embedding2.clone()),
    ];
    repo.bulk_upsert_embeddings(&rows).await.unwrap();

    // Now retrieve them - should get hits
    let result = repo
        .retrieve_embeddings_batch(&[key1.clone(), key2.clone()])
        .await
        .unwrap();

    assert_eq!(result.len(), 2, "Expected 2 hits, got {}", result.len());

    // Check that we got the right embeddings back
    let result_map: std::collections::HashMap<_, _> = result.into_iter().collect();
    assert_eq!(result_map.get(&key1), Some(&embedding1));
    assert_eq!(result_map.get(&key2), Some(&embedding2));
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_retrieve_partial_hits(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    // Ensure model
    let model_key = EmbeddingModelKey {
        provider: "test-provider",
        name: "text-embedding-3-small".to_string(),
        revision: Some("v1"),
    };
    let model_row = repo.ensure_model(&model_key, 1536).await.unwrap();

    // Insert only one embedding
    let key_exists = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("cached data"),
        input_text: "cached data".to_string(),
    };
    let embedding_exists = create_mock_embedding(1536);

    repo.bulk_upsert_embeddings(&[(key_exists.clone(), embedding_exists.clone())])
        .await
        .unwrap();

    // Create a key that doesn't exist
    let key_missing = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("not cached"),
        input_text: "not cached".to_string(),
    };

    // Retrieve both - should only get one hit
    let result = repo
        .retrieve_embeddings_batch(&[key_exists.clone(), key_missing.clone()])
        .await
        .unwrap();

    assert_eq!(
        result.len(),
        1,
        "Expected 1 hit (partial), got {}",
        result.len()
    );

    let result_map: std::collections::HashMap<_, _> = result.into_iter().collect();
    assert_eq!(result_map.get(&key_exists), Some(&embedding_exists));
    assert!(
        !result_map.contains_key(&key_missing),
        "Should not have hit for missing key"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_bulk_upsert_idempotent(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    // Ensure model
    let model_key = EmbeddingModelKey {
        provider: "test-provider",
        name: "text-embedding-3-small".to_string(),
        revision: Some("v1"),
    };
    let model_row = repo.ensure_model(&model_key, 1536).await.unwrap();

    // Create test data
    let key = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("test idempotency"),
        input_text: "test idempotency".to_string(),
    };
    let embedding = create_mock_embedding(1536);

    // Insert the same embedding multiple times - should be idempotent
    for _ in 0..3 {
        repo.bulk_upsert_embeddings(&[(key.clone(), embedding.clone())])
            .await
            .unwrap();
    }

    // Should still retrieve it normally
    let result = repo
        .retrieve_embeddings_batch(std::slice::from_ref(&key))
        .await
        .unwrap();

    assert_eq!(result.len(), 1, "Expected 1 hit after multiple upserts");
    assert_eq!(result[0].0, key);
    assert_eq!(result[0].1, embedding);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_multi_model_isolation(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    // Register two different models
    let model1_key = EmbeddingModelKey {
        provider: "openai",
        name: "text-embedding-3-small".to_string(),
        revision: Some("v1"),
    };
    let model1 = repo.ensure_model(&model1_key, 1536).await.unwrap();

    let model2_key = EmbeddingModelKey {
        provider: "openai",
        name: "text-embedding-3-large".to_string(),
        revision: Some("v1"),
    };
    let model2 = repo.ensure_model(&model2_key, 3072).await.unwrap();

    // Create embeddings for the same input text but different models
    let input_text = "hello world";
    let key_model1 = EmbeddingCacheKey {
        model_id: model1.id,
        input_hash: hash_input(input_text),
        input_text: input_text.to_string(),
    };
    let embedding1 = create_mock_embedding(1536);

    let key_model2 = EmbeddingCacheKey {
        model_id: model2.id,
        input_hash: hash_input(input_text),
        input_text: input_text.to_string(),
    };
    let embedding2 = create_mock_embedding(3072);

    // Upsert embeddings for both models
    repo.bulk_upsert_embeddings(&[(key_model1.clone(), embedding1.clone())])
        .await
        .unwrap();
    repo.bulk_upsert_embeddings(&[(key_model2.clone(), embedding2.clone())])
        .await
        .unwrap();

    // Retrieve for model1 - should only get model1's embedding
    let result1 = repo
        .retrieve_embeddings_batch(std::slice::from_ref(&key_model1))
        .await
        .unwrap();
    assert_eq!(result1.len(), 1, "Expected 1 hit for model1");
    assert_eq!(result1[0].0, key_model1);
    assert_eq!(result1[0].1, embedding1);

    // Retrieve for model2 - should only get model2's embedding
    let result2 = repo
        .retrieve_embeddings_batch(std::slice::from_ref(&key_model2))
        .await
        .unwrap();
    assert_eq!(result2.len(), 1, "Expected 1 hit for model2");
    assert_eq!(result2[0].0, key_model2);
    assert_eq!(result2[0].1, embedding2);

    // Try to retrieve model1's embedding using model2's ID (wrong model_id)
    let wrong_key = EmbeddingCacheKey {
        model_id: model2.id, // Wrong model ID
        input_hash: hash_input(input_text),
        input_text: input_text.to_string(),
    };
    // Should get model2's embedding since the key matches model2
    let result_wrong = repo
        .retrieve_embeddings_batch(std::slice::from_ref(&wrong_key))
        .await
        .unwrap();
    assert_eq!(result_wrong.len(), 1, "Expected hit for the actual key");
    assert_eq!(result_wrong[0].0.model_id, model2.id);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_touch_embeddings(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    // Ensure model
    let model_key = EmbeddingModelKey {
        provider: "test-provider",
        name: "text-embedding-3-small".to_string(),
        revision: Some("v1"),
    };
    let model_row = repo.ensure_model(&model_key, 1536).await.unwrap();

    // Insert embedding
    let key = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("touch test"),
        input_text: "touch test".to_string(),
    };
    let embedding = create_mock_embedding(1536);

    repo.bulk_upsert_embeddings(&[(key.clone(), embedding)])
        .await
        .unwrap();

    // Touch the embedding with current time
    let now = Utc::now();
    repo.touch_embeddings(std::slice::from_ref(&key), now)
        .await
        .unwrap();

    // This operation should succeed without errors
    // The actual verification of last_seen_at update would require
    // additional query methods which aren't part of the trait
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_evict_older_than(catalog: &Catalog) {
    let repo = catalog.get_one::<dyn EmbeddingsCacheRepository>().unwrap();

    // Ensure model
    let model_key = EmbeddingModelKey {
        provider: "test-provider",
        name: "text-embedding-3-small".to_string(),
        revision: Some("v1"),
    };
    let model_row = repo.ensure_model(&model_key, 1536).await.unwrap();

    let now = Utc::now();
    let old_time = now - TimeDelta::days(10);
    let recent_time = now - TimeDelta::hours(1);

    // Insert embeddings that will be "old"
    let old_key1 = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("old data 1"),
        input_text: "old data 1".to_string(),
    };
    let old_key2 = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("old data 2"),
        input_text: "old data 2".to_string(),
    };

    // Insert embeddings that will be "recent"
    let recent_key1 = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("recent data 1"),
        input_text: "recent data 1".to_string(),
    };
    let recent_key2 = EmbeddingCacheKey {
        model_id: model_row.id,
        input_hash: hash_input("recent data 2"),
        input_text: "recent data 2".to_string(),
    };

    let embedding = create_mock_embedding(1536);
    repo.bulk_upsert_embeddings(&[
        (old_key1.clone(), embedding.clone()),
        (old_key2.clone(), embedding.clone()),
        (recent_key1.clone(), embedding.clone()),
        (recent_key2.clone(), embedding.clone()),
    ])
    .await
    .unwrap();

    // Touch old embeddings with old timestamp
    repo.touch_embeddings(&[old_key1.clone(), old_key2.clone()], old_time)
        .await
        .unwrap();

    // Touch recent embeddings with recent timestamp
    repo.touch_embeddings(&[recent_key1.clone(), recent_key2.clone()], recent_time)
        .await
        .unwrap();

    // Evict entries older than 5 days ago
    let cutoff = now - TimeDelta::days(5);
    let evicted_count = repo.evict_older_than(cutoff, 100).await.unwrap();

    // Should evict the 2 old entries
    assert_eq!(
        evicted_count, 2,
        "Expected 2 evictions for old data, got {evicted_count}"
    );

    // Verify old entries are gone
    let old_result = repo
        .retrieve_embeddings_batch(&[old_key1, old_key2])
        .await
        .unwrap();
    assert!(
        old_result.is_empty(),
        "Expected no hits for evicted old data, got {} results",
        old_result.len()
    );

    // Verify recent entries are still present
    let recent_result = repo
        .retrieve_embeddings_batch(&[recent_key1.clone(), recent_key2.clone()])
        .await
        .unwrap();
    assert_eq!(
        recent_result.len(),
        2,
        "Expected 2 recent entries to remain, got {}",
        recent_result.len()
    );

    // Verify the remaining entries are the correct ones
    let result_map: std::collections::HashMap<_, _> = recent_result.into_iter().collect();
    assert!(
        result_map.contains_key(&recent_key1),
        "Recent key1 should still be present"
    );
    assert!(
        result_map.contains_key(&recent_key2),
        "Recent key2 should still be present"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn hash_input(text: &str) -> [u8; 32] {
    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    hasher.finalize().into()
}

fn create_mock_embedding(dims: usize) -> Vec<u8> {
    // Create a mock embedding as packed f32 values in little-endian
    let mut bytes = Vec::with_capacity(dims * 4);
    #[allow(clippy::cast_precision_loss)]
    for i in 0..dims {
        let value = (i as f32) / (dims as f32);
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    bytes
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
