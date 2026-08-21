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
use kamu_resources_postgres::{
    PostgresResourceLabelProjectionRepository,
    PostgresResourceRepository,
};
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
    fixture =
        resource_repo_suite::test_create_find_update_resource_with_populated_labels_annotations,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_snapshots_by_ids,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_handles_by_ids,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_snapshots_by_schema_and_ids,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles_exact_ids,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles_any_type,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles_per_row_account,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles_per_row_labels,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles_any_type_labels,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles_pattern_special_characters,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_count_search_resource_handles,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_count_search_resource_handles_exact_ids,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_list_resource_snapshots_label_filtering,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles_label_filtering,
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
    fixture = resource_repo_suite::test_list_resource_snapshots_by_scope,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_list_resource_snapshots_with_queries,
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

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_account_rename_reflected_immediately_in_headers,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_deleted_account_falls_back_to_sentinel_name,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_search_resource_handles_partially_vacuous_scope,
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
        catalog_builder.add::<PostgresResourceLabelProjectionRepository>();
        catalog_builder.add::<PostgresAccountRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_ids_by_schema_and_label_returns_nothing_initially,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_ids_by_schema_and_label_orders_by_created_at,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_ids_by_schema_and_label_discriminates_schema_and_value,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_ids_by_schema_and_label_excludes_deleted,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = postgres,
    fixture = resource_repo_suite::test_find_resource_ids_by_schema_and_label_spans_accounts,
    harness = PostgresResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
