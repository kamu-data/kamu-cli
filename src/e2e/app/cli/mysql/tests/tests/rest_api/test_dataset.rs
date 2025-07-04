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
    fixture = kamu_cli_e2e_repo_tests::rest_api::test_datasets_by_id,
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = mysql,
    fixture = kamu_cli_e2e_repo_tests::rest_api::test_datasets_endpoint_restricted_for_anonymous,
    options = Options::default()
        .with_multi_tenant()
        .with_kamu_config(MULTITENANT_KAMU_CONFIG_WITH_RESTRICTED_ANONYMOUS)
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
