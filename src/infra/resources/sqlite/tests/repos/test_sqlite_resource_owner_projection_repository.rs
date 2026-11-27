// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::SqliteTransactionManager;
use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use dill::{Catalog, CatalogBuilder};
use kamu_resources_repo_tests::resource_owner_projection_repository_test_suite as owner_projection_repo_suite;
use kamu_resources_sqlite::{SqliteResourceOwnerProjectionRepository, SqliteResourceRepository};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = owner_projection_repo_suite::test_replace_owners,
    harness = SqliteResourceOwnerProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = sqlite,
    fixture = owner_projection_repo_suite::test_rolled_back_replace_keeps_previous_owners,
    harness = SqliteResourceOwnerProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteResourceOwnerProjectionRepositoryHarness {
    catalog: Catalog,
}

impl SqliteResourceOwnerProjectionRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteResourceOwnerProjectionRepository>();
        catalog_builder.add::<SqliteResourceRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
