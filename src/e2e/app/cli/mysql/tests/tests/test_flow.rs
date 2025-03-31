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
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_gql_get_dataset_list_flows,
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_gql_dataset_all_flows_paused,
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_gql_dataset_flows_initiators,
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_gql_dataset_trigger_flow,
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_ingest,
    options = Options::default()
        .with_frozen_system_time()
        .with_kamu_config(indoc::indoc!(
            r#"
        kind: CLIConfig
        version: 1
        content:
          flowSystem:
            flowAgent:
              awaitingStepSecs: 1
              mandatoryThrottlingPeriodSecs: 5
            taskAgent:
              taskCheckingIntervalSecs: 1
        "#
        )),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_ingest_no_polling_source,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_execute_transform,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_execute_transform_no_set_transform,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_hard_compaction,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_trigger_flow_reset,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_flow_planing_failure,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
