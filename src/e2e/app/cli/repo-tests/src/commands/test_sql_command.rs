// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::read_dir;

use kamu_cli_e2e_common::{
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
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
            indoc::indoc!(
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

    kamu.assert_failure_command_execution(
        ["list"],
        None,
        Some(["Error: Directory is not a kamu workspace"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_sql_command(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution(
        [
            "sql",
            "--command",
            "SELECT 42 as answer;",
            "--output-format",
            "table",
        ],
        Some(indoc::indoc!(
            r#"
            ┌────────┐
            │ answer │
            ├────────┤
            │     42 │
            └────────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        [
            "sql",
            "--command",
            "SELECT * FROM \"player-scores\";",
            "--output-format",
            "table",
        ],
        Some(indoc::indoc!(
            r#"
            ┌┐
            ││
            ├┤
            ││
            └┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.execute_with_input(
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await
    .success();

    kamu.assert_success_command_execution(
        [
            "sql",
            "--command",
            "SELECT * FROM \"player-scores\" ORDER BY offset;",
            "--output-format",
            "table",
        ],
        Some(indoc::indoc!(
            r#"
            ┌────────┬────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
            │ offset │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
            ├────────┼────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
            │      0 │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
            │      1 │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
            └────────┴────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_sql_command_exports_to_parquet(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    let mut games = String::new();
    for game_id in 0..50_000 {
        games.push_str(&format!(
            r#"
            {{"match_time": "2000-01-01", "match_id": {game_id}, "player_id": "Alice", "score": 100}}
            {{"match_time": "2000-01-01", "match_id": {game_id}, "player_id": "Bob", "score": 80}}
            "#
        ));
    }

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        games,
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported.parquet");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "sql",
            "--command",
            "select * from 'player-scores'",
            "--output-format",
            "parquet",
            "--output-path",
            output_path_str,
        ],
        None,
        Some(["Exported 100000 rows"]),
    )
    .await;

    assert!(output_path.exists(), "Parquet file should be created");
    assert!(
        output_path.is_file(),
        "All the data should be stored to a single file"
    );

    let output_path = kamu.workspace_path().join("exported");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "sql",
            "--command",
            "select * from 'player-scores'",
            "--output-format",
            "parquet",
            "--output-path",
            output_path_str,
            "--records-per-file",
            "20000",
        ],
        None,
        Some(["Exported 100000 rows"]),
    )
    .await;

    assert!(output_path.exists(), "Parquet files should be created");
    assert!(
        output_path.is_dir(),
        "Data should be stored into separate files"
    );
    assert!(
        read_dir(&output_path).unwrap().count() > 1,
        "Should be several files"
    );
}

pub async fn test_sql_command_export_errors(kamu: KamuCliPuppet) {
    kamu.assert_failure_command_execution(
        [
            "sql",
            "--command",
            "select * from 'player-scores'",
            "--output-format",
            "parquet",
            "--output-path",
            "output_path",
            "--records-per-file",
            "20000",
            "--engine",
            "spark",
        ],
        None,
        Some([r#"Data export to file\(s\) is available with DataFusion \(default\) engine only"#]),
    )
    .await;

    kamu.assert_failure_command_execution(
        [
            "sql",
            "--command",
            "select * from 'player-scores'",
            "--output-format",
            "table",
            "--output-path",
            "output_path",
        ],
        None,
        Some([
            "Invalid output format for export 'table'. Supported formats: 'csv', 'ndjson', \
             'parquet'",
        ]),
    )
    .await;

    kamu.assert_failure_command_execution(
        [
            "sql",
            "--command",
            "select * from 'player-scores'",
            "--output-format",
            "parquet",
            "--records-per-file",
            "20000",
        ],
        None,
        Some([r#"Partitioning is only supported for data export to file\(s\)"#]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["sql", "--command", "select 1", "--output-format", "parquet"],
        None,
        Some(["Parquet format is applicable for data export only"]),
    )
    .await;
}
