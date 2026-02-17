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
use kamu_flow_system_inmem::InMemoryFlowProcessState;
use time_source::SystemTimeSourceDefault;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_empty_state_table_initially,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_in_initial_state,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_immediate_stop,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_in_failing_state,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_recovery,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_index_single_process_after_pause,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_unfiltered,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_scope,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_flow_type,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_effective_status,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_combined_filters,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_unconfigured_edge_cases,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_last_attempt_between,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_last_failure_since,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_next_planned_before,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_next_planned_after,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_filtered_by_min_consecutive_failures,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_combined_time_filters,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_rollup_from_csv_combined_failures_and_time,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_from_csv_unfiltered_with_default_ordering,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_last_attempt_at_nulls_last,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_next_planned_at_nulls_last,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_last_failure_at_nulls_last,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_consecutive_failures,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_effective_state,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_ordering_flow_type,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_edge_cases,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_with_filters,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_different_orderings,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination_boundary_conditions,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_flow_types,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_effective_states,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_last_attempt_between,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_last_failure_since,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_planned_before,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_planned_after,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_consecutive_failures,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_filter_by_scope,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_combined_filters,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_delete_process,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_delete_process_with_history,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_delete_multiple_process_types_by_scope,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture =
        kamu_flow_system_repo_tests::test_flow_process_state::test_dataset_page_recent_activity,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_account_dashboard_triage,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

database_transactional_test!(
    storage = inmem,
    fixture = kamu_flow_system_repo_tests::test_flow_process_state::test_account_dashboard_upcoming_updates,
    harness = InMemoryFlowProcessStateHarness
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

struct InMemoryFlowProcessStateHarness {
    catalog: Catalog,
}

impl InMemoryFlowProcessStateHarness {
    pub fn new() -> Self {
        let mut catalog_builder = CatalogBuilder::new();
        catalog_builder.add::<InMemoryFlowProcessState>();
        catalog_builder.add::<SystemTimeSourceDefault>();

        Self {
            catalog: catalog_builder.build(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
