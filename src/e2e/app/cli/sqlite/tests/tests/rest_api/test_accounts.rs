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
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::rest_api::test_accounts_me_kamu_user,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::rest_api::test_accounts_me_e2e_user,
    options = Options::default().with_multi_tenant()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::rest_api::test_accounts_me_unauthorized,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::rest_api::test_create_account_and_modify_password,
    options = Options::default()
        .with_multi_tenant()
        .with_kamu_config(indoc::indoc!(
            r#"
        kind: CLIConfig
        version: 1
        content:
          users:
            predefined:
              - accountName: kamu
                email: kamu@example.com
                isAdmin: true
                canProvisionAccounts: true
        "#
        ))
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
