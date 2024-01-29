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

#[test]
fn test_datafusion_cli() {
    let expected_output =
        "+----------+\n| Int64(1) |\n+----------+\n| 1        |\n+----------+\n1 row in set.";

    let mut cmd = Command::cargo_bin("kamu-cli").unwrap();

    let assert = cmd.arg("sql").write_stdin("select 1;").assert();

    let output = assert.get_output();

    //not using assert.stdout("expected text") as time of query execution may
    // differ
    let string_result = String::from_utf8_lossy(&output.stdout);

    assert!(
        string_result.contains(expected_output),
        "Output does not contain expected text"
    );
    assert.success();
}
