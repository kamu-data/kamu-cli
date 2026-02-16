// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PostgresTransactionManager;
use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_search_cache_postgres::PostgresEmbeddingsCacheRepository;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = kamu_search_cache_repo_tests::test_ensure_model,
    harness = PostgresEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_search_cache_repo_tests::test_retrieve_embeddings_batch_empty,
    harness = PostgresEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_search_cache_repo_tests::test_bulk_upsert_and_retrieve,
    harness = PostgresEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_search_cache_repo_tests::test_retrieve_partial_hits,
    harness = PostgresEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_search_cache_repo_tests::test_bulk_upsert_idempotent,
    harness = PostgresEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_search_cache_repo_tests::test_multi_model_isolation,
    harness = PostgresEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_search_cache_repo_tests::test_touch_embeddings,
    harness = PostgresEmbeddingsCacheRepositoryHarness
);

database_transactional_test!(
    storage = postgres,
    fixture = kamu_search_cache_repo_tests::test_evict_older_than,
    harness = PostgresEmbeddingsCacheRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresEmbeddingsCacheRepositoryHarness {
    catalog: Catalog,
}

impl PostgresEmbeddingsCacheRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        // Initialize catalog with predefined Postgres pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresEmbeddingsCacheRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
