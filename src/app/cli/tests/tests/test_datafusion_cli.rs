// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use assert_cmd::Command;
use indoc::indoc;
use kamu_cli_wrapper::Kamu;

////////////////////////////////////////////////////////////////////////////////

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_datafusion_cli() {
    let kamu = Kamu::new_workspace_tmp().await;

    let assert = Command::cargo_bin(env!("CARGO_PKG_NAME"))
        .unwrap()
        .current_dir(kamu.workspace_path())
        .arg("sql")
        .write_stdin("select 1;")
        .assert();

    let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

    assert!(
        stdout.contains(
            indoc!(
                r#"
                +----------+
                | Int64(1) |
                +----------+
                | 1        |
                +----------+
                "#
            )
            .trim()
        ),
        "Unexpected output:\n{stdout}",
    );

    assert.success();
}

#[test_group::group(engine, datafusion)]
#[test_log::test(tokio::test)]
async fn test_datafusion_cli_not_launched_in_root_ws() {
    // this test checks that workspace was not created in root directory
    Command::cargo_bin(env!("CARGO_PKG_NAME"))
        .unwrap()
        .arg("list")
        .assert()
        .failure();
}

////////////////////////////////////////////////////////////////////////////////
