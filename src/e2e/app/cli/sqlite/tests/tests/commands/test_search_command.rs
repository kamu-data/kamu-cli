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
// test_search_multi_user
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_search_multi_user_st
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(
            indoc::indoc!(
                r#"
                kind: CLIConfig
                version: 1
                content:
                  users:
                    predefined:
                      - accountName: kamu
                        email: kamu@example.com
                "#
            )
        ),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_search_multi_user_mt
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(
            indoc::indoc!(
                r#"
                kind: CLIConfig
                version: 1
                content:
                  users:
                    predefined:
                      - accountName: kamu
                        email: kamu@example.com
                "#
            )
        ),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_search_by_name
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_search_by_name_st
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
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_search_by_name_mt
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_search_by_repo
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_search_by_repo_st
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
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_search_by_repo_mt
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time(),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
