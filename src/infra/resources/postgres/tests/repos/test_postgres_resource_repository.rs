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
use kamu_resources_postgres::PostgresResourceRepository;
use kamu_resources_repo_tests::resource_repository_test_suite as resource_repo_suite;
use sqlx::PgPool;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_no_resources_initially,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_create_and_find_resource,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_snapshots_by_uids,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_snapshots_by_kind_and_uids,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_identities,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_create_resource_duplicate_fails,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_create_resource_duplicate_ignore_case_fails,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_update_resource,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_update_resource_wrong_event_id_fails,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_update_resource_optimistic_locking,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_update_resources,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_update_resources_wrong_event_id_fails,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_list_resource_ids_with_pagination,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_list_resource_snapshots_by_kind,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_list_all_resource_snapshots,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_count_resources,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_summarize_resources,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_deleted_resource_not_returned,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_resource_name_case_insensitive,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct PostgresResourceRepositoryHarness {
    catalog: Catalog,
}

impl PostgresResourceRepositoryHarness {
    pub fn new(pg_pool: PgPool) -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(pg_pool);
        catalog_builder.add::<PostgresTransactionManager>();
        catalog_builder.add::<PostgresResourceRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
