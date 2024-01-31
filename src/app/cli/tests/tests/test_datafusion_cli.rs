// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

/////////////////////////////////////////////////////////////////////////////////////////

use assert_cmd::Command;

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_datafusion_cli() {
    let mut cmd = Command::cargo_bin("kamu-cli").unwrap();

    let assert = cmd.arg("sql").write_stdin("select 1;").assert();
    assert.failure().code(0).stderr("hello\n");
}
