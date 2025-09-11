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
use kamu_flow_system_sqlite::{SqliteFlowProcessStateQuery, SqliteFlowProcessStateRepository};
use sqlx::SqlitePool;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_empty_state_table_initially,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_in_initial_state,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_immediate_stop,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_in_failing_state,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_recovery,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_pause,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_unfiltered,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_scope,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_flow_type,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_effective_status,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_combined_filters,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_from_csv_unfiltered_with_default_ordering,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_last_attempt_at_nulls_last,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_next_planned_at_nulls_last,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_last_failure_at_nulls_last,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_consecutive_failures,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_effective_state,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_name_alpha,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_flow_type,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_edge_cases,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_with_filters,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_different_orderings,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_boundary_conditions,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_flow_types,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_effective_states,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_last_attempt_between,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_last_failure_since,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_planned_before,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_planned_after,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_consecutive_failures,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_name_contains,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_scope,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_combined_filters,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_delete_process,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_delete_process_with_history,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_delete_multiple_process_types_by_scope,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_dataset_page_recent_activity,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_account_dashboard_triage,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_account_dashboard_upcoming_updates,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = sqlite,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_failing_webhooks_for_dataset_sorted_az,
    harness = SqliteFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Harness
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct SqliteFlowProcessStateHarness {
    catalog: Catalog,
}

impl SqliteFlowProcessStateHarness {
    pub fn new(sqlite_pool: SqlitePool) -> Self {
        // Initialize catalog with predefined SQLite pool
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add_value(sqlite_pool);
        catalog_builder.add::<SqliteTransactionManager>();
        catalog_builder.add::<SqliteFlowProcessStateQuery>();
        catalog_builder.add::<SqliteFlowProcessStateRepository>();
        catalog_builder.add::<SystemTimeSourceDefault>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
