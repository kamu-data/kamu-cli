// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use indoc::indoc;
use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_ingest_from_file_ledger(kamu: KamuCliPuppet) {
    kamu.add_dataset(odf::DatasetSnapshot {
        name: "population".try_into().unwrap(),
        kind: odf::DatasetKind::Root,
        metadata: vec![odf::metadata::AddPushSource {
            source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: odf::metadata::ReadStepNdJson {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "city STRING".to_owned(),
                    "population BIGINT".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: odf::metadata::MergeStrategyLedger {
                primary_key: vec!["event_time".to_owned(), "city".to_owned()],
            }
            .into(),
        }
        .into()],
    })
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(
        &data_path,
        indoc!(
            "
            2020-01-01,A,1000
            2020-01-01,B,2000
            2020-01-01,C,3000
            "
        ),
    )
    .unwrap();

    kamu.execute([
        "ingest",
        "population",
        "--input-format",
        "csv",
        path(&data_path),
    ])
    .await
    .success();

    kamu.assert_last_data_slice(
        &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("population")),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 0      | 0  | 2050-01-02T03:04:05Z | 2020-01-01T00:00:00Z | A    | 1000       |
            | 1      | 0  | 2050-01-02T03:04:05Z | 2020-01-01T00:00:00Z | B    | 2000       |
            | 2      | 0  | 2050-01-02T03:04:05Z | 2020-01-01T00:00:00Z | C    | 3000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_ingest_from_file_snapshot_with_event_time(kamu: KamuCliPuppet) {
    kamu.add_dataset(odf::DatasetSnapshot {
        name: "population".try_into().unwrap(),
        kind: odf::DatasetKind::Root,
        metadata: vec![odf::metadata::AddPushSource {
            source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: odf::metadata::ReadStepNdJson {
                schema: Some(vec![
                    "city STRING".to_owned(),
                    "population BIGINT".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: odf::metadata::MergeStrategySnapshot {
                primary_key: vec!["city".to_owned()],
                compare_columns: None,
            }
            .into(),
        }
        .into()],
    })
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(
        &data_path,
        indoc!(
            "
            A,1000
            B,2000
            C,3000
            "
        )
        .trim(),
    )
    .unwrap();

    kamu.execute([
        "ingest",
        "population",
        "--input-format",
        "csv",
        "--event-time",
        "2050-01-01T00:00:00Z",
        path(&data_path),
    ])
    .await
    .success();

    kamu.assert_last_data_slice(
        &odf::DatasetAlias::new(None, odf::DatasetName::new_unchecked("population")),
        indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 event_time (TIMESTAMP(MILLIS,true));
              OPTIONAL BYTE_ARRAY city (STRING);
              OPTIONAL INT64 population;
            }
            "#
        ),
        indoc!(
            r#"
            +--------+----+----------------------+----------------------+------+------------+
            | offset | op | system_time          | event_time           | city | population |
            +--------+----+----------------------+----------------------+------+------------+
            | 0      | 0  | 2050-01-02T03:04:05Z | 2050-01-01T00:00:00Z | A    | 1000       |
            | 1      | 0  | 2050-01-02T03:04:05Z | 2050-01-01T00:00:00Z | B    | 2000       |
            | 2      | 0  | 2050-01-02T03:04:05Z | 2050-01-01T00:00:00Z | C    | 3000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ingest_from_stdin(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    assert_ingest_data_to_player_scores_from_stdio(
        &kamu,
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
        indoc::indoc!(
            r#"
            ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
            │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
            ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
            └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
            "#
        ),
    )
    .await;

    assert_ingest_data_to_player_scores_from_stdio(
        &kamu,
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
        indoc::indoc!(
            r#"
            ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
            │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
            ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-02T00:00:00Z │        2 │     Alice │    70 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-02T00:00:00Z │        2 │   Charlie │    90 │
            └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
            "#
        ),
    )
    .await;

    assert_ingest_data_to_player_scores_from_stdio(
        &kamu,
        ["ingest", "player-scores", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3,
        indoc::indoc!(
            r#"
            ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
            │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
            ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-02T00:00:00Z │        2 │     Alice │    70 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-02T00:00:00Z │        2 │   Charlie │    90 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-03T00:00:00Z │        3 │       Bob │    60 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-03T00:00:00Z │        3 │   Charlie │   110 │
            └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ingest_recursive(kamu: KamuCliPuppet) {
    // 0. Add datasets: the root dataset and its derived dataset
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.assert_failure_command_execution(
        ["tail", "leaderboard", "--output-format", "table"],
        None,
        Some(["Error: Dataset schema is not yet available: leaderboard"]),
    )
    .await;

    // TODO: `kamu ingest`: implement `--recursive` mode
    //        https://github.com/kamu-data/kamu-cli/issues/886

    // 1. Ingest data: the first chunk
    // {
    //     let assert = kamu
    //         .execute_with_input(
    //             ["ingest", "player-scores", "--stdin", "--recursive"],
    //             DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    //         )
    //         .await
    //         .success();
    //
    //     let stderr =
    // std::str::from_utf8(&assert.get_output().stderr).unwrap();
    //
    //     assert!(
    //         stderr.contains("Dataset updated"),
    //         "Unexpected output:\n{stderr}",
    //     );
    // }

    // TODO: check via the tail command added data in the derived dataset
    //       (leaderboard)

    // TODO: do the same for 2nd & 3rd chunks
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ingest_with_source_name(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    assert_ingest_data_to_player_scores_from_stdio(
        &kamu,
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
        indoc::indoc!(
            r#"
            ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
            │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
            ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
            └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
            "#
        ),
    )
    .await;

    assert_ingest_data_to_player_scores_from_stdio(
        &kamu,
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
        indoc::indoc!(
            r#"
            ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
            │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
            ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-02T00:00:00Z │        2 │     Alice │    70 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-02T00:00:00Z │        2 │   Charlie │    90 │
            └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
            "#
        ),
    )
    .await;

    assert_ingest_data_to_player_scores_from_stdio(
        &kamu,
        [
            "ingest",
            "player-scores",
            "--stdin",
            "--source-name",
            "default",
        ],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3,
        indoc::indoc!(
            r#"
            ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
            │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
            ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-02T00:00:00Z │        2 │     Alice │    70 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-02T00:00:00Z │        2 │   Charlie │    90 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-03T00:00:00Z │        3 │       Bob │    60 │
            │  0 │ 2050-01-02T03:04:05Z │ 2000-01-03T00:00:00Z │        3 │   Charlie │   110 │
            └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn path(p: &Path) -> &str {
    p.as_os_str().to_str().unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_ingest_data_to_player_scores_from_stdio<I, S, T>(
    kamu: &KamuCliPuppet,
    ingest_cmd: I,
    ingest_data: T,
    expected_tail_table: &str,
) where
    I: IntoIterator<Item = S> + Send + Clone,
    S: AsRef<std::ffi::OsStr>,
    T: Into<Vec<u8>> + Send + Clone,
{
    // Ingest
    kamu.assert_success_command_execution_with_input(
        ingest_cmd.clone(),
        ingest_data.clone(),
        None,
        Some(["Dataset updated"]),
    )
    .await;

    // Trying to ingest the same data
    kamu.assert_success_command_execution_with_input(
        ingest_cmd,
        ingest_data,
        None,
        Some(["Dataset up-to-date"]),
    )
    .await;

    // Assert ingested data
    kamu.assert_player_scores_dataset_data(expected_tail_table)
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
