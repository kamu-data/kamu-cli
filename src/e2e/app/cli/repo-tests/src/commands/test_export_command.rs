// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fs::read_dir;

use datafusion::arrow::array::{Int64Array, RecordBatch, downcast_array};
use datafusion::execution::config::SessionConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::options::{CsvReadOptions, NdJsonReadOptions, ParquetReadOptions};
use kamu_cli_e2e_common::DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR;
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn generate_game(game_id: u32) -> String {
    format!(
        r#"
        {{"match_time": "2000-01-01", "match_id": {game_id}, "player_id": "Alice", "score": 100}}
        {{"match_time": "2000-01-01", "match_id": {game_id}, "player_id": "Bob", "score": 80}}
        "#
    )
}

fn generate_games(num: u32) -> String {
    let mut games = String::new();
    for game_id in 0..num {
        games.push_str(&generate_game(game_id));
    }
    games
}

fn check_order(batches: Vec<RecordBatch>, start_offset: i64) -> i64 {
    let mut total = 0;
    let mut expected = start_offset;
    for batch in batches {
        let offsets_dyn = batch.column_by_name("offset").unwrap();
        let offsets = downcast_array::<Int64Array>(offsets_dyn);
        for i in 0..offsets.len() {
            let offset = offsets.value(i);
            assert_eq!(
                offset, expected,
                "Offset column in parquet file is not sequentially ordered"
            );
            expected += 1;
            total += 1;
        }
    }
    total
}

fn create_ctx() -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(1);
    SessionContext::new_with_config(config)
}

pub async fn test_export_to_csv_file(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(50_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported.csv");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "csv",
            "--output-path",
            output_path_str,
        ],
        None,
        Some(["Exported 100000 rows"]), // 1 game has 2 rows
    )
    .await;

    assert!(output_path.exists(), "CSV file should be created");
    assert!(
        output_path.is_file(),
        "All the data should be stored to a single file"
    );

    let df = create_ctx()
        .read_csv(output_path.to_str().unwrap(), CsvReadOptions::new())
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    check_order(batches, 0);
}

pub async fn test_export_to_csv_file_partitioning_ignored_for_single_file(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(10_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported.csv");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "csv",
            "--output-path",
            output_path_str,
            "--records-per-file",
            "7000",
        ],
        None,
        Some(["Exported 20000 rows"]), // 1 game has 2 rows
    )
    .await;

    assert!(output_path.exists(), "CSV file should be created");
    assert!(
        output_path.is_file(),
        "All the data should be stored to a single file"
    );
}

pub async fn test_export_to_csv_files(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(10_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported_csv");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "csv",
            "--output-path",
            output_path_str,
            "--records-per-file",
            "15000",
        ],
        None,
        Some(["Exported 20000 rows"]), // 1 game has 2 rows
    )
    .await;

    assert!(output_path.exists(), "CSV files should be created");
    assert!(
        output_path.is_dir(),
        "Data should be stored into separate files"
    );
    assert!(
        read_dir(&output_path).unwrap().count() > 1,
        "Should be several files"
    );

    let mut files: Vec<_> = read_dir(&output_path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();

    let mut start_offset = 0;
    for file in files {
        let df = create_ctx()
            .read_csv(file.to_str().unwrap(), CsvReadOptions::new())
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        // Make sure every file is ordered
        let count = check_order(batches, start_offset);
        start_offset += count;
    }

    // When path with extension part has trailing path separator,
    // it's considered as a path
    let output_path = kamu.workspace_path().join("exported.csv/");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "csv",
            "--output-path",
            output_path_str,
            "--records-per-file",
            "15000",
        ],
        None,
        Some(["Exported 20000 rows"]),
    )
    .await;

    assert!(output_path.exists(), "CSV files should be created");
    assert!(
        output_path.is_dir(),
        "Data should be stored into separate files"
    );
    assert!(
        read_dir(&output_path).unwrap().count() > 1,
        "Should be several files"
    );
}

pub async fn test_export_to_json_file(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(50_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported.json");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "ndjson",
            "--output-path",
            output_path_str,
        ],
        None,
        Some(["Exported 100000 rows"]),
    )
    .await;

    assert!(output_path.exists(), "JSON file should be created");
    assert!(
        output_path.is_file(),
        "All the data should be stored to a single file"
    );

    let df = create_ctx()
        .read_json(output_path.to_str().unwrap(), NdJsonReadOptions::default())
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    check_order(batches, 0);
}

pub async fn test_export_to_json_file_partitioning_ignored_for_single_file(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(10_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported.json");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "ndjson",
            "--output-path",
            output_path_str,
            "--records-per-file",
            "7000",
        ],
        None,
        Some(["Exported 20000 rows"]),
    )
    .await;

    assert!(output_path.exists(), "JSON file should be created");
    assert!(
        output_path.is_file(),
        "All the data should be stored to a single file"
    );
}

pub async fn test_export_to_json_files(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(20_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported_json");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "ndjson",
            "--output-path",
            output_path_str,
            "--records-per-file",
            "15000",
        ],
        None,
        Some(["Exported 40000 rows"]),
    )
    .await;

    assert!(output_path.exists(), "JSON files should be created");
    assert!(
        output_path.is_dir(),
        "Data should be stored into separate files"
    );
    assert!(
        read_dir(&output_path).unwrap().count() > 1,
        "Should be several files"
    );

    let mut files: Vec<_> = read_dir(&output_path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();

    let mut start_offset = 0;
    for file in files {
        let df = create_ctx()
            .read_json(file.to_str().unwrap(), NdJsonReadOptions::default())
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let count = check_order(batches, start_offset);
        start_offset += count;
    }
}

pub async fn test_export_to_parquet_file(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(50_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported.parquet");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
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

    let df = create_ctx()
        .read_parquet(output_path.to_str().unwrap(), ParquetReadOptions::new())
        .await
        .unwrap();
    let batches = df.collect().await.unwrap();
    check_order(batches, 0);
}

pub async fn test_export_to_parquet_file_partitioning_ignored_for_single_file(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(10_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported.parquet");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "parquet",
            "--output-path",
            output_path_str,
            "--records-per-file",
            "7000",
        ],
        None,
        Some(["Exported 20000 rows"]),
    )
    .await;

    assert!(output_path.exists(), "Parquet file should be created");
    assert!(
        output_path.is_file(),
        "All the data should be stored to a single file"
    );
}

pub async fn test_export_to_parquet_files(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(20_000),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported_parquet");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_success_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "parquet",
            "--output-path",
            output_path_str,
            "--records-per-file",
            "15000",
        ],
        None,
        Some(["Exported 40000 rows"]),
    )
    .await;

    assert!(output_path.exists(), "Parquet filess should be created");
    assert!(
        output_path.is_dir(),
        "Data should be stored into separate files"
    );
    assert!(
        read_dir(&output_path).unwrap().count() > 1,
        "Should be several files"
    );

    let mut files: Vec<_> = read_dir(&output_path)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    files.sort();

    let mut start_offset = 0;
    for file in files {
        let df = create_ctx()
            .read_parquet(file.to_str().unwrap(), ParquetReadOptions::new())
            .await
            .unwrap();

        let batches = df.collect().await.unwrap();
        let count = check_order(batches, start_offset);
        start_offset += count;
    }
}

pub async fn test_export_to_unsupported_format(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        generate_games(10),
    )
    .await
    .success();

    let output_path = kamu.workspace_path().join("exported.xsl");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_failure_command_execution(
        [
            "export",
            "player-scores",
            "--output-format",
            "xls",
            "--output-path",
            output_path_str,
        ],
        None,
        Some([
            "error: invalid value 'xls' for '--output-format <OUTPUT_FORMAT>'",
            "Supported formats: 'parquet', 'csv', 'ndjson'",
        ]),
    )
    .await;
}

pub async fn test_export_non_existent_dataset(kamu: KamuCliPuppet) {
    let output_path = kamu.workspace_path().join("exported.parquet");
    let output_path_str = output_path.as_os_str().to_str().unwrap();

    kamu.assert_failure_command_execution(
        [
            "export",
            "non-existent",
            "--output-format",
            "parquet",
            "--output-path",
            output_path_str,
        ],
        None,
        Some(["Error: Dataset not found: non-existent"]),
    )
    .await;
}
