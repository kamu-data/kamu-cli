// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PostgresTransactionManager;
use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use dill::{Catalog, CatalogBuilder};
use kamu_resources_postgres::{
    PostgresResourceOwnerProjectionRepository,
    PostgresResourceRepository,
};
use kamu_resources_repo_tests::resource_owner_projection_repository_test_suite as owner_projection_repo_suite;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = owner_projection_repo_suite::test_replace_owners,
    harness = PostgresResourceOwnerProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = postgres,
    fixture = owner_projection_repo_suite::test_rolled_back_replace_keeps_previous_owners,
    harness = PostgresResourceOwnerProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresResourceOwnerProjectionRepositoryHarness {
    catalog: Catalog,
}

impl PostgresResourceOwnerProjectionRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresResourceOwnerProjectionRepository>();
        catalog_builder.add::<PostgresResourceRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
