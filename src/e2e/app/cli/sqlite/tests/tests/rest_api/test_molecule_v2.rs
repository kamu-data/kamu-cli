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
    fixture = kamu_cli_e2e_repo_tests::rest_api::test_molecule_v2_data_room_quota_exceeded,
    options = Options::default()
        .with_multi_tenant()
        .with_kamu_config(kamu_cli_e2e_repo_tests::rest_api::MULTITENANT_MOLECULE_QUOTA_CONFIG),
    extra_test_groups = "containerized, quota, molecule"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_run_api_server_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::rest_api::test_molecule_v2_announcements_quota_exceeded,
    options = Options::default()
        .with_multi_tenant()
        .with_kamu_config(kamu_cli_e2e_repo_tests::rest_api::MULTITENANT_MOLECULE_QUOTA_CONFIG),
    extra_test_groups = "containerized, quota, molecule"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
