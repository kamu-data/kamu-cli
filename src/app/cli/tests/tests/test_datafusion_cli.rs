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

#[test_log::test(tokio::test)]
async fn test_datafusion_cli() {
    let mut cmd = Command::cargo_bin("kamu-cli").unwrap();

    let assert = cmd.arg("sql").write_stdin("select 1;");

    let output = assert.output().unwrap();

    let string_result = String::from_utf8_lossy(&output.stdout);
    let string_error = String::from_utf8_lossy(&output.stderr);
    let string_status = output.status.to_string();

    println!("Subprocess stdout: {:?}", string_result);
    println!("Subprocess stderror: {:?}", string_error);
    println!("Subprocess stdstatus: {:?}", string_status);
}
