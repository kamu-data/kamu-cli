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

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::test_repository_pull_aliases_commands
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::test_repository_push_aliases_commands
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
