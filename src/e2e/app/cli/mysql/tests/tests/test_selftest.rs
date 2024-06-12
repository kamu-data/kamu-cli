// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{kamu_cli_e2e_test, mysql_kamu_cli_e2e_test};

////////////////////////////////////////////////////////////////////////////////

kamu_cli_e2e_test!(
    mysql_kamu_cli_e2e_test,
    kamu_cli_e2e_repo_tests,
    test_selftest
);

////////////////////////////////////////////////////////////////////////////////
