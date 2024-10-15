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
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_sequence,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_force_push_pull,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_add_alias,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_pull_as,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_all,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion, risingwave"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_recursive,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion, risingwave"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_pull_set_watermark,
    options = Options::default().with_frozen_system_time(),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_pull_reset_derivative,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion, risingwave"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_visibility,
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_push_pull_s3,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_smart_pull_derivative,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion, risingwave"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
