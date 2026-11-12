// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common_macros::{database_concurrent_transactions_test, database_transactional_test};
use kamu_resources_repo_tests::resource_repository_test_suite as resource_repo_suite;

use super::InMemoryResourceRepositoriesHarness;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_no_resources_initially,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_create_and_find_resource,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture =
        resource_repo_suite::test_create_find_update_resource_with_populated_labels_annotations,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_find_resource_snapshots_by_ids,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_find_resource_handles_by_ids,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_find_resource_snapshots_by_schema_and_ids,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles_exact_ids,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles_any_type,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles_per_row_account,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles_per_row_labels,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles_any_type_labels,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles_pattern_special_characters,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_count_search_resource_handles,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_count_search_resource_handles_exact_ids,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_list_resource_snapshots_label_filtering,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles_label_filtering,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_create_resource_duplicate_fails,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_create_resource_duplicate_ignore_case_fails,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_update_resource,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_status_generation_round_trips,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_update_resource_wrong_event_id_fails,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_update_resource_optimistic_locking,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_update_resources,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_update_resources_wrong_event_id_fails,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_list_resource_snapshots_by_scope,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_list_resource_snapshots_with_queries,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_list_all_resource_snapshots,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_summarize_resources,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_find_deleted_resource_not_returned,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_resource_being_deleted_round_trips,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_resource_being_deleted_is_hidden_unless_included,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_name_of_resource_being_deleted_can_be_taken,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_summarize_resources_counts_deleting,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_resource_name_case_insensitive,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_account_rename_reflected_immediately_in_headers,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_deleted_account_falls_back_to_sentinel_name,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_search_resource_handles_partially_vacuous_scope,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_rolled_back_create_leaves_no_resource,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_create_same_name_in_concurrent_transactions,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture = resource_repo_suite::test_create_name_freed_by_concurrent_deletion_request,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_concurrent_transactions_test!(
    storage = inmem,
    fixture =
        resource_repo_suite::test_create_name_twice_while_concurrent_deletion_request_frees_it,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture =
        resource_repo_suite::test_find_resource_ids_by_schema_and_label_returns_nothing_initially,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_find_resource_ids_by_schema_and_label_orders_by_created_at,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_find_resource_ids_by_schema_and_label_discriminates_schema_and_value,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture = resource_repo_suite::test_find_resource_ids_by_schema_and_label_excludes_deleted,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture =
        resource_repo_suite::test_find_resource_ids_by_schema_and_label_excludes_other_accounts,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture =
        resource_repo_suite::test_find_resource_ids_by_schema_and_label_any_account_spans_accounts,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem_with_transaction,
    fixture =
        resource_repo_suite::test_find_resource_ids_by_schema_and_label_any_account_discriminates,
    harness = InMemoryResourceRepositoriesHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
