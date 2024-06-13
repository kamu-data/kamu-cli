// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::prelude::*;

////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    mysql,
    kamu_cli_e2e_repo_tests,
    test_login_password_predefined_successful
);

////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(mysql, kamu_cli_e2e_repo_tests, test_login_enabled_methods);

////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    mysql,
    kamu_cli_e2e_repo_tests,
    test_login_dummy_github,
    Options {
        is_multi_tenant: true,
        env_vars: Some(vec![
            ("KAMU_AUTH_GITHUB_CLIENT_ID".into(), "1".into()),
            ("KAMU_AUTH_GITHUB_CLIENT_SECRET".into(), "2".into())
        ]),
    }
);

////////////////////////////////////////////////////////////////////////////////
