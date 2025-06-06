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
// test_login_logout_password
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_logout_password_st,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_logout_password_mt,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_logout_oauth
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_logout_oauth_st,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_logout_oauth_mt,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_add_repo
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_add_repo_st,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_add_repo_mt,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_password_add_repo
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_password_add_repo_st,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_password_add_repo_mt,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_oauth_add_repo
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_oauth_add_repo_st,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_oauth_add_repo_mt,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_interactive_successful
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_interactive_successful_st,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_interactive_successful_mt,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_login_interactive_device_code_expired
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_interactive_device_code_expired_st,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::commands::test_login_interactive_device_code_expired_mt,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
