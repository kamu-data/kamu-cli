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
    fixture = kamu_cli_e2e_repo_tests::test_webhook_delivered_signed,
    options = Options::default().with_kamu_config(kamu_cli_e2e_repo_tests::WEBHOOKS_E2E_CONFIG),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_webhook_delivery_failed_on_unsuccessful_response,
    options = Options::default().with_kamu_config(kamu_cli_e2e_repo_tests::WEBHOOKS_E2E_CONFIG),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_webhook_delivery_failed_to_connect,
    options = Options::default().with_kamu_config(kamu_cli_e2e_repo_tests::WEBHOOKS_E2E_CONFIG),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_webhook_delivery_timed_out,
    options = Options::default().with_kamu_config(kamu_cli_e2e_repo_tests::WEBHOOKS_E2E_CONFIG),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_webhook_delivery_retried_until_success,
    options = Options::default()
        .with_kamu_config(kamu_cli_e2e_repo_tests::WEBHOOKS_WITH_RETRY_E2E_CONFIG),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_webhook_subscription_unreachable_until_reactivated,
    options = Options::default().with_kamu_config(kamu_cli_e2e_repo_tests::WEBHOOKS_E2E_CONFIG),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_webhook_signed_with_rotated_secret,
    options = Options::default().with_kamu_config(kamu_cli_e2e_repo_tests::WEBHOOKS_E2E_CONFIG),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = postgres,
    fixture = kamu_cli_e2e_repo_tests::test_webhook_external_target_rejected,
    options = Options::default().with_kamu_config(kamu_cli_e2e_repo_tests::WEBHOOKS_E2E_CONFIG),
    extra_test_groups = "engine, ingest, datafusion"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
