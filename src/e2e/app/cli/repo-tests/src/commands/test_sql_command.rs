// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use indoc::indoc;
use kamu_cli_e2e_common::{
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
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

    {
        let assert = kamu.execute(["list"]).await.failure();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Error: Directory is not a kamu workspace"),
            "Unexpected output:\n{stderr}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_sql_command(kamu: KamuCliPuppet) {
    {
        let assert = kamu
            .execute([
                "sql",
                "--command",
                "SELECT 42 as answer;",
                "--output-format",
                "table",
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            indoc::indoc!(
                r#"
                ┌────────┐
                │ answer │
                ├────────┤
                │     42 │
                └────────┘
                "#
            ),
            stdout
        );
    }

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    {
        let assert = kamu
            .execute([
                "sql",
                "--command",
                "SELECT * FROM \"player-scores\";",
                "--output-format",
                "table",
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            indoc::indoc!(
                r#"
                ┌┐
                ││
                ├┤
                ││
                └┘
                "#
            ),
            stdout
        );
    }

    kamu.execute_with_input(
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await
    .success();

    {
        let assert = kamu
            .execute([
                "sql",
                "--command",
                "SELECT * FROM \"player-scores\" ORDER BY offset;",
                "--output-format",
                "table",
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            indoc::indoc!(
                r#"
                ┌────────┬────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
                │ offset │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
                ├────────┼────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
                │      0 │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
                │      1 │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
                └────────┴────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
                "#
            ),
            stdout
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
