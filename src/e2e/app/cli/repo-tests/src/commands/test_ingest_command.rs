// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::Path;

use chrono::{TimeZone, Utc};
use indoc::indoc;
use kamu_cli_e2e_common::{
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_ingest_from_file_ledger(mut kamu: KamuCliPuppet) {
    kamu.set_system_time(Some(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()));

    kamu.add_dataset(DatasetSnapshot {
        name: "population".try_into().unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![AddPushSource {
            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: ReadStepNdJson {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "city STRING".to_owned(),
                    "population BIGINT".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: MergeStrategyLedger {
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
        &DatasetName::new_unchecked("population"),
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
            | 0      | 0  | 2000-01-01T00:00:00Z | 2020-01-01T00:00:00Z | A    | 1000       |
            | 1      | 0  | 2000-01-01T00:00:00Z | 2020-01-01T00:00:00Z | B    | 2000       |
            | 2      | 0  | 2000-01-01T00:00:00Z | 2020-01-01T00:00:00Z | C    | 3000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

pub async fn test_push_ingest_from_file_snapshot_with_event_time(mut kamu: KamuCliPuppet) {
    kamu.set_system_time(Some(Utc.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap()));

    kamu.add_dataset(DatasetSnapshot {
        name: "population".try_into().unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![AddPushSource {
            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: ReadStepNdJson {
                schema: Some(vec![
                    "city STRING".to_owned(),
                    "population BIGINT".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: MergeStrategySnapshot {
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
        &DatasetName::new_unchecked("population"),
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
            | 0      | 0  | 2000-01-01T00:00:00Z | 2050-01-01T00:00:00Z | A    | 1000       |
            | 1      | 0  | 2000-01-01T00:00:00Z | 2050-01-01T00:00:00Z | B    | 2000       |
            | 2      | 0  | 2000-01-01T00:00:00Z | 2050-01-01T00:00:00Z | C    | 3000       |
            +--------+----+----------------------+----------------------+------+------------+
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_ingest_from_stdin(mut kamu: KamuCliPuppet) {
    kamu.set_system_time(Some(Utc.with_ymd_and_hms(2050, 2, 3, 4, 5, 6).unwrap()));

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    ingest_data_to_player_scores_from_stdio(
        &kamu,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    ingest_data_to_player_scores_from_stdio(
        &kamu,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;

    ingest_data_to_player_scores_from_stdio(
        &kamu,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3,
    )
    .await;

    {
        let assert = kamu
            .execute(["tail", "player-scores", "--output-format", "table"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            stdout,
            indoc::indoc!(
                r#"
                ┌────────┬────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
                │ offset │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
                ├────────┼────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
                │      0 │ +A │ 2050-02-03T04:05:06Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
                │      1 │ +A │ 2050-02-03T04:05:06Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
                │      2 │ +A │ 2050-02-03T04:05:06Z │ 2000-01-02T00:00:00Z │        2 │   Charlie │    90 │
                │      3 │ +A │ 2050-02-03T04:05:06Z │ 2000-01-02T00:00:00Z │        2 │     Alice │    70 │
                │      4 │ +A │ 2050-02-03T04:05:06Z │ 2000-01-03T00:00:00Z │        3 │       Bob │    60 │
                │      5 │ +A │ 2050-02-03T04:05:06Z │ 2000-01-03T00:00:00Z │        3 │   Charlie │   110 │
                └────────┴────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
                "#
            )
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn path(p: &Path) -> &str {
    p.as_os_str().to_str().unwrap()
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn ingest_data_to_player_scores_from_stdio(kamu: &KamuCliPuppet, input: &str) {
    // Ingest
    {
        let assert = kamu
            .execute_with_input(["ingest", "player-scores", "--stdin"], input)
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Dataset updated"),
            "Unexpected output:\n{stderr}",
        );
    }
    // Trying to ingest the same data
    {
        let assert = kamu
            .execute_with_input(["ingest", "player-scores", "--stdin"], input)
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Dataset up-to-date"),
            "Unexpected output:\n{stderr}",
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
