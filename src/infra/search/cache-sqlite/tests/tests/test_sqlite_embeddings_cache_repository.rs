// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::SqliteTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_search_cache_sqlite::SqliteEmbeddingsCacheRepository;
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_search_cache_repo_tests::test_ensure_model,
    harness = SqliteEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_search_cache_repo_tests::test_retrieve_embeddings_batch_empty,
    harness = SqliteEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_search_cache_repo_tests::test_bulk_upsert_and_retrieve,
    harness = SqliteEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_search_cache_repo_tests::test_retrieve_partial_hits,
    harness = SqliteEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_search_cache_repo_tests::test_bulk_upsert_idempotent,
    harness = SqliteEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_search_cache_repo_tests::test_multi_model_isolation,
    harness = SqliteEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_search_cache_repo_tests::test_touch_embeddings,
    harness = SqliteEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_search_cache_repo_tests::test_evict_older_than,
    harness = SqliteEmbeddingsCacheRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteEmbeddingsCacheRepositoryHarness {
    catalog: Catalog,
}

impl SqliteEmbeddingsCacheRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined SQLite pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteEmbeddingsCacheRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
