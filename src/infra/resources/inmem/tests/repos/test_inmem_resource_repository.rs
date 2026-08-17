// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::database_transactional_test;
use dill::{Catalog, CatalogBuilder};
use kamu_accounts_inmem::InMemoryAccountRepository;
use kamu_resources_inmem::{InMemoryResourceLabelProjectionRepository, InMemoryResourceRepository};
use kamu_resources_repo_tests::resource_repository_test_suite as resource_repo_suite;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_no_resources_initially,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_create_and_find_resource,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        resource_repo_suite::test_create_find_update_resource_with_populated_labels_annotations,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_find_resource_snapshots_by_ids,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_find_resource_handles_by_ids,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_find_resource_snapshots_by_schema_and_ids,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_search_resource_handles,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_search_resource_handles_exact_ids,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_search_resource_handles_any_type,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_search_resource_handles_per_row_account,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_search_resource_handles_per_row_labels,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_search_resource_handles_any_type_labels,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_search_resource_handles_pattern_special_characters,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_count_search_resource_handles,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_count_search_resource_handles_exact_ids,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_list_resource_snapshots_label_filtering,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_search_resource_handles_label_filtering,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_create_resource_duplicate_fails,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_create_resource_duplicate_ignore_case_fails,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_update_resource,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_update_resource_wrong_event_id_fails,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_update_resource_optimistic_locking,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_update_resources,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_update_resources_wrong_event_id_fails,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_list_resource_ids_with_pagination,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_list_resource_snapshots_by_scope,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_list_resource_snapshots_with_queries,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_list_all_resource_snapshots,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_count_resources,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_summarize_resources,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_find_deleted_resource_not_returned,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_resource_name_case_insensitive,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_account_rename_reflected_immediately_in_headers,
    harness = InMemoryResourceRepositoryHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryResourceRepositoryHarness {
    catalog: Catalog,
}

impl InMemoryResourceRepositoryHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryResourceRepository>();
        catalog_builder.add::<InMemoryResourceLabelProjectionRepository>();
        catalog_builder.add::<InMemoryAccountRepository>();
        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
