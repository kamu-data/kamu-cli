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
use kamu_resources_postgres::{
    PostgresResourceLabelProjectionRepository,
    PostgresResourceRepository,
};
use kamu_resources_repo_tests::resource_label_projection_repository_test_suite as label_projection_repo_suite;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = label_projection_repo_suite::test_no_entries_initially,
    harness = PostgresResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = label_projection_repo_suite::test_replace_entries_then_find,
    harness = PostgresResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = label_projection_repo_suite::test_replace_entries_overwrites_previous_set,
    harness = PostgresResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = label_projection_repo_suite::test_replace_entries_with_empty_slice_clears,
    harness = PostgresResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = label_projection_repo_suite::test_entries_isolated_by_resource_id,
    harness = PostgresResourceLabelProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresResourceLabelProjectionRepositoryHarness {
    catalog: Catalog,
}

impl PostgresResourceLabelProjectionRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresResourceLabelProjectionRepository>();
        catalog_builder.add::<PostgresResourceRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
