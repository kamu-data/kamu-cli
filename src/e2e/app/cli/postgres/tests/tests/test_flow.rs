// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::prelude::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_gql_get_dataset_list_flows,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_gql_dataset_flow_processes,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_gql_dataset_flows_initiators,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_gql_dataset_trigger_flow,
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_ingest,
    options = Options::default()
        .with_frozen_system_time()
        .with_kamu_config(KAMU_CONFIG_WITH_FAST_FLOW_SYSTEM),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_many_ingest_flows_at_once,
    options = Options::default()
        .with_frozen_system_time()
        .with_kamu_config(indoc::indoc!(
            r#"
        kind: CLIConfig
        version: 1
        content:
          flowSystem:
            awaitingStepSecs: 1
          backgroundAgents:
            # Long debounce: all triggers land before the flow agent wakes,
            # so one activation pass pages through more due flows than a page holds
            minDebounceInterval: 1s
            maxListeningTimeout: 1s
            batching:
              flowActivations: 4
        "#
        )),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_ingest_no_polling_source,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_execute_transform,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_execute_transform_no_set_transform,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_hard_compaction,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_reset,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_reset_metadata_only,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_transform_trigger_recovers_from_input_reset_to_metadata,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture =
        kamu_cli_e2e_repo_tests::test_transform_trigger_recovers_from_input_reset_with_new_data,
    // No frozen time: the rebuild transform is throttled after the first one, and a frozen clock
    // never reaches the end of the throttling period
    options = Options::default().with_kamu_config(KAMU_CONFIG_WITH_FAST_FLOW_SYSTEM),
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_flow_planning_failure,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_ingest_flow_lists_reactive_transform_downstream,
    options = Options::default()
        .with_frozen_system_time()
        .with_kamu_config(KAMU_CONFIG_WITH_FAST_FLOW_SYSTEM),
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_schedule_change_reaches_waiting_flow,
    options = Options::default()
        .with_frozen_system_time()
        .with_kamu_config(KAMU_CONFIG_WITH_FAST_FLOW_SYSTEM)
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_config_change_reaches_waiting_flow,
    options = Options::default()
        .with_frozen_system_time()
        .with_kamu_config(KAMU_CONFIG_WITH_FAST_FLOW_SYSTEM)
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_batching_rule_change_reaches_waiting_flow,
    options = Options::default()
        .with_frozen_system_time()
        .with_kamu_config(KAMU_CONFIG_WITH_FAST_FLOW_SYSTEM),
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
