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
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::{AddDatasetOptions, KamuCliPuppetExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_ingest_from_file_ledger(kamu: KamuCliPuppet) {
    kamu.add_dataset(
        odf::DatasetSnapshot {
            name: "population".try_into().unwrap(),
            kind: odf::DatasetKind::Root,
            metadata: vec![
                odf::metadata::AddPushSource {
                    source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(odf::schema::DataSchema::new(vec![
                            odf::schema::DataField::timestamp_millis_utc("event_time"),
                            odf::schema::DataField::string("city"),
                            odf::schema::DataField::i64("population"),
                        ])),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: None,
                    merge: odf::metadata::MergeStrategyLedger {
                        primary_key: vec!["event_time".to_owned(), "city".to_owned()],
                    }
                    .into(),
                }
                .into(),
            ],
        },
        AddDatasetOptions::default(),
    )
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
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY city (STRING);
              REQUIRED INT64 population;
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
    kamu.add_dataset(
        odf::DatasetSnapshot {
            name: "population".try_into().unwrap(),
            kind: odf::DatasetKind::Root,
            metadata: vec![
                odf::metadata::AddPushSource {
                    source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(odf::schema::DataSchema::new(vec![
                            odf::schema::DataField::string("city"),
                            odf::schema::DataField::i64("population"),
                        ])),
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
                .into(),
            ],
        },
        AddDatasetOptions::default(),
    )
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
              REQUIRED INT64 event_time (TIMESTAMP(MILLIS,true));
              REQUIRED BYTE_ARRAY city (STRING);
              REQUIRED INT64 population;
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

    kamu.assert_success_command_execution(
        ["tail", "leaderboard", "--output-format", "table"],
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
    S: AsRef<std::ffi::OsStr> + Send,
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

pub async fn test_ingest_into_other_accounts_dataset_fails(mut kamu: KamuCliPuppet) {
    let [alice, bob] = kamu.create_accounts(["alice", "bob"]).await;

    kamu.set_account(Some(bob));
    for (name, visibility) in [("bob-root", "private"), ("bob-public", "public")] {
        kamu.execute_with_input(
            ["add", "--stdin", "--name", name, "--visibility", visibility],
            DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
        )
        .await
        .success();
    }

    // A dataset alice cannot read is not found for her, a readable one is denied
    kamu.set_account(Some(alice));
    for (name, expected_error) in [
        ("bob/bob-root", "Dataset not found: bob/bob-root"),
        (
            "bob/bob-public",
            "User has no 'write' permission in dataset 'bob/bob-public'",
        ),
    ] {
        kamu.assert_failure_command_execution_with_input(
            ["ingest", name, "--stdin"],
            DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
            None,
            Some([expected_error]),
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ingest_of_invalid_request_fails(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();
    kamu.execute_with_input(
        ["add", "--stdin", "--name", "pulled-scores"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();
    kamu.execute([
        "repo",
        "alias",
        "add",
        "pulled-scores",
        "odf+http://example.com/x",
        "--pull",
    ])
    .await
    .success();

    let data = DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1;
    for (case, cmd, stdin, expected_stderr) in [
        (
            "into a derivative",
            &["ingest", "leaderboard", "--stdin"][..],
            Some(data),
            &["Ingesting data available for root datasets only"][..],
        ),
        (
            "without files or stdin",
            &["ingest", "player-scores"],
            None,
            &["Specify a list of files or pass --stdin"],
        ),
        (
            "unknown source name",
            &[
                "ingest",
                "player-scores",
                "--stdin",
                "--source-name",
                "unknown",
            ],
            Some(data),
            &["Source 'unknown' not found"],
        ),
        (
            "unparsable value",
            &["ingest", "player-scores", "--stdin"],
            Some(
                r#"{"match_time": "2000-01-01", "match_id": "not-a-number", "player_id": "Alice", "score": 100}"#,
            ),
            &[r#"failed to parse "not-a-number" as Int64"#],
        ),
        (
            "into a dataset with a pull alias",
            &["ingest", "pulled-scores", "--stdin"],
            Some(data),
            &[
                "Ingesting data into a dataset that is pulled from a remote will cause histories \
                 to diverge",
                "kamu repo alias list",
            ],
        ),
    ] {
        let stderr = kamu
            .assert_failure_stderr_in_order(case, cmd, stdin, expected_stderr)
            .await
            .stderr;

        // The request is at fault, so it is not reported as an internal error
        assert!(
            !stderr.contains("Internal error"),
            "Case '{case}': reported as an internal error\n{stderr}"
        );
    }

    // No failed request leaves data behind
    pretty_assertions::assert_eq!(
        vec![0; 3],
        kamu.list_datasets()
            .await
            .into_iter()
            .map(|dataset| dataset.records)
            .collect::<Vec<_>>()
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
