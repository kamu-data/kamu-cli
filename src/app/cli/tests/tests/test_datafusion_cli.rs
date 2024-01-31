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

    let output = assert.get_output();

    let string_result = String::from_utf8_lossy(&output.stdout);
    let string_error = String::from_utf8_lossy(&output.stderr);
    let string_status = output.status.to_string();

    assert_eq!(string_status, "exit status: 0");
    assert_eq!(string_error, "Kamu + DataFusion SQL shell. Type \\? for help.\n");
    assert_eq!(string_result, "+----------+\n| Int64(1) |\n+----------+\n| 1        |\n+----------+\n1 row in set. Query took 0.011 seconds.\n\n\\q\n");
}
