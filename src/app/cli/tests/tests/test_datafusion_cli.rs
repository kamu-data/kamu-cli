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
use regex::Regex;

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_datafusion_cli() {
    let mut cmd = Command::cargo_bin("kamu-cli").unwrap();

    let assert = cmd.arg("sql").write_stdin("select 1;").assert();

    let output = assert.get_output();

    let string_result = String::from_utf8_lossy(&output.stdout);

    let seconds = Regex::new(r"took (.*?) seconds.")
        .unwrap()
        .captures(&*string_result)
        .and_then(|caps| caps.get(1))
        .map(|m| m.as_str())
        .unwrap_or("Seconds not found.");

    let expected_output = format!(
        indoc::indoc! {r#"
            +----------+
            | Int64(1) |
            +----------+
            | 1        |
            +----------+
            1 row in set. Query took {:.5} seconds.

            \q
        "#},
        seconds
    );

    assert_eq!(string_result.trim(), expected_output.trim());
    assert.success();
}
