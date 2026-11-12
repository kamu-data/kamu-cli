// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::prelude::*;
use kamu_cli_e2e_repo_tests::commands::KAMU_CONFIG_WITH_SHORT_SHUTDOWN_TIMEOUT;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_api_server_exits_on_sigterm,
    options = Options::default()
        .with_multi_tenant()
        .with_kamu_config(KAMU_CONFIG_WITH_SHORT_SHUTDOWN_TIMEOUT),
    sqlite_only = "checks how the process handles a signal; the repository type cannot change it"
);

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::commands::test_api_server_exits_on_sigterm_during_a_transfer,
    options = Options::default()
        .with_multi_tenant()
        .with_kamu_config(KAMU_CONFIG_WITH_SHORT_SHUTDOWN_TIMEOUT),
    sqlite_only = "checks how the process handles a signal; the repository type cannot change it"
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
