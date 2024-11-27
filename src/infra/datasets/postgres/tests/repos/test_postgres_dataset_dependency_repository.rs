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
use kamu_accounts_postgres::PostgresAccountRepository;
use kamu_datasets_postgres::{PostgresDatasetDependencyRepository, PostgresDatasetEntryRepository};
use kamu_datasets_repo_tests::dataset_dependency_repo;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_dependency_repo::test_crud_single_dependency,
    harness = PostgresDatasetDependencyRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_dependency_repo::test_several_unrelated_dependencies,
    harness = PostgresDatasetDependencyRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_dependency_repo::test_dependency_chain,
    harness = PostgresDatasetDependencyRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_dependency_repo::test_dependency_fanins,
    harness = PostgresDatasetDependencyRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = dataset_dependency_repo::test_dependency_fanouts,
    harness = PostgresDatasetDependencyRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresDatasetDependencyRepositoryHarness {
    catalog: Catalog,
}

impl PostgresDatasetDependencyRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();

        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();

        catalog_builder.add::<PostgresAccountRepository>();
        catalog_builder.add::<PostgresDatasetEntryRepository>();
        catalog_builder.add::<PostgresDatasetDependencyRepository>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
