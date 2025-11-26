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
// test_smart_push_smart_pull_sequence
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_smart_pull_sequence_st_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_smart_pull_sequence_st_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_smart_pull_sequence_mt_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_smart_pull_sequence_mt_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_push_force_smart_pull_force
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_force_smart_pull_force_st_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_force_smart_pull_force_st_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_force_smart_pull_force_mt_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_force_smart_pull_force_mt_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_push_no_alias_smart_pull_no_alias
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_no_alias_smart_pull_no_alias_st_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_no_alias_smart_pull_no_alias_st_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_no_alias_smart_pull_no_alias_mt_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_no_alias_smart_pull_no_alias_mt_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_pull_as
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_pull_as_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_pull_as_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_pull_visibility_public
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_pull_visibility_public_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_pull_visibility_public_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_push_all_smart_pull_all
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_all_smart_pull_all_st_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_all_smart_pull_all_st_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_all_smart_pull_all_mt_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_all_smart_pull_all_mt_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_push_recursive_smart_pull_recursive
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_recursive_smart_pull_recursive_st_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_recursive_smart_pull_recursive_st_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_recursive_smart_pull_recursive_mt_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_recursive_smart_pull_recursive_mt_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_push_visibility
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_visibility_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_visibility_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_simple_push_to_s3_smart_pull
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_simple_push_to_s3_smart_pull_st_st,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_simple_push_to_s3_smart_pull_st_mt,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_simple_push_to_s3_smart_pull_mt_st,
    options = Options::default()
        .with_multi_tenant()
        .with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_simple_push_to_s3_smart_pull_mt_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_push_to_registered_repo_smart_pull
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_with_registered_repo_smart_pull_st_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_with_registered_repo_smart_pull_st_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_with_registered_repo_smart_pull_mt_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_with_registered_repo_smart_pull_mt_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_trigger_dependent_dataset_update_st,
    options = Options::default()
        .with_multi_tenant()
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
                  checkingIntervalSecs: 1
            "#
        )),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_trigger_dependent_dataset_update_mt,
    options = Options::default()
        .with_multi_tenant()
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
                  checkingIntervalSecs: 1
            "#
        )),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_smart_push_smart_pull_force_overwrite_seed_block
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_smart_pull_force_overwrite_seed_block_st_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_smart_pull_force_overwrite_seed_block_st_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_smart_pull_force_overwrite_seed_block_mt_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_smart_pull_force_overwrite_seed_block_mt_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_derived_with_unaccessible_inputs_resolved_via_gql_st,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_derived_with_unaccessible_inputs_resolved_via_gql_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
