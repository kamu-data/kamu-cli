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
use kamu_configuration_postgres::PostgresSecretSetProjectionRepository;
use kamu_configuration_repo_tests::secret_set_projection_repository_test_suite as secret_set_repo;
use kamu_resources_postgres::PostgresResourceRepository;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_entries_empty_initially,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_replace_and_get_entries,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_find_entry_by_key,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_entries_isolated_by_resource,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_entries_isolated_by_generation,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_get_latest_entries_before_generation,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_replace_entries_concurrent_modification,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_cleanup_entries_before_generation,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_cleanup_does_not_affect_other_resources,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_replace_preserves_stable_identity_and_creation_time,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_get_latest_entries,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = secret_set_repo::test_delete_all_entries,
    harness = PostgresSecretSetProjectionRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresSecretSetProjectionRepositoryHarness {
    catalog: Catalog,
}

impl PostgresSecretSetProjectionRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresResourceRepository>();
        catalog_builder.add::<PostgresSecretSetProjectionRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
