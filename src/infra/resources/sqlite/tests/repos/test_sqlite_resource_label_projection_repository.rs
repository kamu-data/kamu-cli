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
use kamu_resources_repo_tests::resource_label_projection_repository_test_suite as label_projection_repo_suite;
use kamu_resources_sqlite::{SqliteResourceLabelProjectionRepository, SqliteResourceRepository};
use sqlx::SqlitePool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = label_projection_repo_suite::test_no_entries_initially,
    harness = SqliteResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = label_projection_repo_suite::test_replace_entries_then_find,
    harness = SqliteResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = label_projection_repo_suite::test_replace_entries_overwrites_previous_set,
    harness = SqliteResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = label_projection_repo_suite::test_replace_entries_with_empty_slice_clears,
    harness = SqliteResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = label_projection_repo_suite::test_entries_isolated_by_resource_id,
    harness = SqliteResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteResourceLabelProjectionRepositoryHarness {
    catalog: Catalog,
}

impl SqliteResourceLabelProjectionRepositoryHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteResourceLabelProjectionRepository>();
        catalog_builder.add::<SqliteResourceRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
