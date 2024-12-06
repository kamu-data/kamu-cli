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
// test_pull_env_var_template_default_value
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_env_var_template_default_value_st,
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_env_var_template_default_value_mt,
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_env_var_template_default_value_missing_values
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_env_var_template_default_value_missing_values_st,
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_env_var_template_default_value_missing_values_mt,
    extra_test_groups = "containerized, engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_set_watermark
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_set_watermark_st,
    options = Options::default().with_frozen_system_time(),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_set_watermark_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_frozen_system_time(),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_reset_derivative
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_reset_derivative_st,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_reset_derivative_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_pull_derivative
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_derivative_st,
    options = Options::default().with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_pull_derivative_mt,
    options = Options::default()
        .with_multi_tenant()
        .with_frozen_system_time(),
    extra_test_groups = "containerized, engine, ingest, transform, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
