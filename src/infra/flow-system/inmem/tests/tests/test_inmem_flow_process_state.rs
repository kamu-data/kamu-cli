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
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_from_csv_unfiltered,
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
        kamu_flow_system_repo_tests::test_flow_process_state::test_list_processes_pagination,
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
