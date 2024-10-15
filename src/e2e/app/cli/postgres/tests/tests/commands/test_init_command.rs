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
    fixture = kamu_cli_e2e_repo_tests::test_init_exist_ok_st,
    options = Options::default().with_no_workspace()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::test_init_exist_ok_mt,
    options = Options::default().with_no_workspace()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

kamu_cli_execute_command_e2e_test!(
    storage = sqlite,
    fixture = kamu_cli_e2e_repo_tests::test_init_in_an_existing_workspace,
    options = Options::default().with_no_workspace()
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
