// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu::testing::LocalS3Server;
use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_NAME,
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::DatasetAlias;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_SNAPSHOT_STR: &str = indoc::indoc!(
    r#"
    kind: DatasetSnapshot
    version: 1
    content:
      name: test.pull-from-file
      kind: Root
      metadata:
        - kind: SetPollingSource
          fetch:
            kind: Url
            url: file://${{ env.data_dir || env.workspace_dir }}/${{ env.data_file || 'data.csv' }}
          read:
            kind: Csv
            header: true
            separator: ','
          merge:
            kind: snapshot
            primaryKey:
              - city
    "#
);

const DATASET_INGEST_DATA: &str = indoc::indoc!(
    r#"
    city,population
    A,1000
    B,2000
    C,3000
    "#
);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_env_var_template_default_value(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_SNAPSHOT_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: test.pull-from-file
            Added 1 dataset(s)
            "#
        )]),
    )
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(&data_path, DATASET_INGEST_DATA).unwrap();

    kamu.assert_success_command_execution_with_env(
        ["pull", "test.pull-from-file"],
        vec![("workspace_dir".as_ref(), kamu.workspace_path().as_os_str())],
        None,
        Some([indoc::indoc!(
            r#"
                1 dataset(s) updated
            "#
        )]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_env_var_template_default_value_missing_values(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_SNAPSHOT_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: test.pull-from-file
            Added 1 dataset(s)
            "#
        )]),
    )
    .await;

    let data_path = kamu.workspace_path().join("data.csv");
    std::fs::write(&data_path, DATASET_INGEST_DATA).unwrap();

    kamu.assert_failure_command_execution(
        ["pull", "test.pull-from-file"],
        None,
        Some([indoc::indoc!(
            r#"
                Failed to pull test.pull-from-file: Missing values for variable(s): 'env.data_dir || env.workspace_dir'
            "#
        )]),
    )
        .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_set_watermark(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        [
            "pull",
            dataset_name.as_str(),
            "--set-watermark",
            "2051-01-02T03:04:05Z",
        ],
        None,
        Some(["Committed new block"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_reset_derivative(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();
    let dataset_derivative_name = DATASET_DERIVATIVE_LEADERBOARD_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    kamu.assert_success_command_execution(
        ["pull", dataset_derivative_name.as_str()],
        None,
        Some(["1 dataset(s) updated"]),
    )
    .await;

    let expected_derivative_schema = indoc::indoc!(
        r#"
        message arrow_schema {
          OPTIONAL INT64 offset;
          REQUIRED INT32 op;
          REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
          OPTIONAL INT64 match_time (TIMESTAMP(MILLIS,true));
          OPTIONAL INT64 place (INTEGER(64,false));
          OPTIONAL INT64 match_id;
          OPTIONAL BYTE_ARRAY player_id (STRING);
          OPTIONAL INT64 score;
        }
        "#
    );
    let expected_derivative_data = indoc::indoc!(
        r#"
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | offset | op | system_time          | match_time           | place | match_id | player_id | score |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
        | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        "#
    );
    kamu.assert_last_data_slice(
        &DatasetAlias::new(None, dataset_derivative_name.clone()),
        expected_derivative_schema,
        expected_derivative_data,
    )
    .await;

    // Compact root dataset
    kamu.execute([
        "--yes",
        "system",
        "compact",
        dataset_name.as_str(),
        "--hard",
        "--keep-metadata-only",
    ])
    .await
    .success();

    // Pull derivative should fail
    kamu.assert_failure_command_execution(
        ["pull", dataset_derivative_name.as_str()],
        None,
        Some(["Failed to update 1 dataset(s)"]),
    )
    .await;

    // Add new data to root dataset
    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;

    kamu.assert_success_command_execution(
        [
            "pull",
            dataset_derivative_name.as_str(),
            "--reset-derivatives-on-diverged-input",
        ],
        None,
        Some(["1 dataset(s) updated"]),
    )
    .await;

    let expected_derivative_data = indoc::indoc!(
        r#"
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | offset | op | system_time          | match_time           | place | match_id | player_id | score |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 1     | 2        | Charlie   | 90    |
        | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2     | 2        | Alice     | 70    |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        "#
    );
    kamu.assert_last_data_slice(
        &DatasetAlias::new(None, dataset_derivative_name),
        expected_derivative_schema,
        expected_derivative_data,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_pull_derivative(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();
    let dataset_derivative_name = DATASET_DERIVATIVE_LEADERBOARD_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    kamu.assert_failure_command_execution(
        [
            "tail",
            dataset_derivative_name.as_str(),
            "--output-format",
            "table",
        ],
        None,
        Some(["Error: Dataset schema is not yet available: leaderboard"]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["pull", dataset_derivative_name.as_str()],
        None,
        Some(["1 dataset(s) updated"]),
    )
    .await;

    kamu.assert_player_scores_dataset_data(indoc::indoc!(
        r#"
        ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
        │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
        ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
        │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
        │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
        └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
        "#
    ))
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_push_pull_s3(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    let s3_server = LocalS3Server::new().await;
    let dataset_url = format!("{}/e2e-user/{dataset_name}", s3_server.url);

    // Push dataset
    kamu.assert_success_command_execution(
        ["push", dataset_name.as_str(), "--to", dataset_url.as_str()],
        None,
        Some(["1 dataset(s) pushed"]),
    )
    .await;

    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", dataset_url.as_str()],
                None,
                Some(["1 dataset(s) updated"]),
            )
            .await;

        let expected_schema = indoc::indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_id;
              OPTIONAL BYTE_ARRAY player_id (STRING);
              OPTIONAL INT64 score;
            }
            "#
        );
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1        | Alice     | 100   |
            | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        kamu.assert_last_data_slice(
            &DatasetAlias::new(None, dataset_name),
            expected_schema,
            expected_data,
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
