// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_search_cache_inmem::InMemoryEmbeddingsCacheRepository;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_search_cache_repo_tests::test_ensure_model,
    harness = InMemoryEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_search_cache_repo_tests::test_retrieve_embeddings_batch_empty,
    harness = InMemoryEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_search_cache_repo_tests::test_bulk_upsert_and_retrieve,
    harness = InMemoryEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_search_cache_repo_tests::test_retrieve_partial_hits,
    harness = InMemoryEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_search_cache_repo_tests::test_bulk_upsert_idempotent,
    harness = InMemoryEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_search_cache_repo_tests::test_multi_model_isolation,
    harness = InMemoryEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_search_cache_repo_tests::test_touch_embeddings,
    harness = InMemoryEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = inmem,
    fixture = kamu_search_cache_repo_tests::test_evict_older_than,
    harness = InMemoryEmbeddingsCacheRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryEmbeddingsCacheRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryEmbeddingsCacheRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryEmbeddingsCacheRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
