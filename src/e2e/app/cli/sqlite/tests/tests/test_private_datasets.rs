// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::prelude::*;
use kamu_cli_e2e_repo_tests::private_datasets::PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_datasets_have_correct_visibility_after_creation,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_minimum_dataset_maintainer_can_change_dataset_visibility,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_is_available_only_to_authorized_users_in_the_search_results,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_is_available_in_queries_only_to_authorized_users,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_is_available_only_to_the_owner_or_admin_in_a_users_dataset_list,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_as_a_downstream_dependency_is_visible_only_to_the_owner_or_admin,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_as_an_upstream_dependency_is_visible_only_to_the_owner_or_admin,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_private_dataset_can_only_be_pulled_by_the_owner_or_admin,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_public_derivative_dataset_that_has_a_private_dependency_can_be_pulled_by_anyone,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::private_datasets::test_a_dataset_that_has_a_private_dependency_can_only_be_pulled_in_workspace_by_the_owner_or_admin,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture =
        kamu_cli_e2e_repo_tests::private_datasets::test_minimum_dataset_maintainer_can_access_roles,
    // We need synthetic time for the tests, but the third-party JWT code
    // uses the current time. Assuming that the token lifetime is 24 hours, we will
    // use the projected date (the current day) as a workaround.
    options = Options::default()
        .with_multi_tenant()
        .with_today_as_frozen_system_time()
        .with_kamu_config(PRIVATE_DATESET_WORKSPACE_KAMU_CONFIG),
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
