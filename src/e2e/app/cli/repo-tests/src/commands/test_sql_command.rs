// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indoc::indoc;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_datafusion_cli(kamu: KamuCliPuppet) {
    let assert = kamu
        .execute_with_input(["sql"], "select 1;")
        .await
        .success();

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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_datafusion_cli_not_launched_in_root_ws(kamu: KamuCliPuppet) {
    // This test checks that workspace was not created in root (kamu-cli) directory.
    //
    // The workspace search functionality checks for parent folders,
    // so there is no problem that the process working directory is one of the
    // subdirectories (kamu-cli/src/e2e/app/cli/inmem)
    kamu.execute(["list"]).await.failure();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
