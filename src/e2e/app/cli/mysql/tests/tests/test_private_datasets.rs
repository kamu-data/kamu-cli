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
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_only_the_dataset_owner_or_admin_can_change_its_visibility,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
              users:
                predefined:
                  - accountName: admin
                    isAdmin: true
                  - accountName: alice
                  - accountName: bob
            "#
        )),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_is_available_only_to_the_owner_or_admin_in_the_search_results,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
              users:
                predefined:
                  - accountName: admin
                    isAdmin: true
                  - accountName: alice
                  - accountName: bob
            "#
        )),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_is_available_in_queries_only_for_the_owner_or_admin,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
              users:
                predefined:
                  - accountName: admin
                    isAdmin: true
                  - accountName: alice
                  - accountName: bob
            "#
        )),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_is_available_only_to_the_owner_or_admin_in_a_users_dataset_list,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
              users:
                predefined:
                  - accountName: admin
                    isAdmin: true
                  - accountName: alice
                  - accountName: bob
            "#
        )),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_as_a_downstream_dependency_is_visible_only_to_the_owner_or_admin,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(indoc::indoc!(
            r#"
            kind: CLIConfig
            version: 1
            content:
              users:
                predefined:
                  - accountName: admin
                    isAdmin: true
                  - accountName: alice
                  - accountName: bob
            "#
        )),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
