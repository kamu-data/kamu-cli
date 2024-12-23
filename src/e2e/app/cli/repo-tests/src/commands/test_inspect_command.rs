// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_NAME,
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_inspect_lineage(kamu: KamuCliPuppet) {
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
        ["inspect", "lineage", "--output-format", "shell"],
        Some(indoc::indoc!(
            r#"
            leaderboard: Derivative
            └── player-scores: Root
            player-scores: Root
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_inspect_query(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    let player_scores_dataset_id = kamu
        .list_datasets()
        .await
        .into_iter()
        .find_map(|dataset| {
            if dataset.name == *DATASET_ROOT_PLAYER_NAME {
                Some(dataset.id)
            } else {
                None
            }
        })
        .unwrap();

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    use odf::metadata::EnumWithVariants;

    let leaderboard_transform_block_hash = kamu
        .list_blocks(&DATASET_DERIVATIVE_LEADERBOARD_NAME)
        .await
        .into_iter()
        .find_map(|block| {
            if block
                .block
                .event
                .as_variant::<odf::metadata::SetTransform>()
                .is_some()
            {
                Some(block.block_hash)
            } else {
                None
            }
        })
        .unwrap();

    kamu.assert_success_command_execution(
        ["inspect", "query", "player-scores"],
        Some(""),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        ["inspect", "query", "leaderboard"],
        Some(
            indoc::formatdoc!(
                r#"
                Transform: {leaderboard_transform_block_hash}
                As Of: 2050-01-02T03:04:05Z
                Inputs:
                  player_scores  {player_scores_dataset_id}
                Engine: datafusion (None)
                Query: leaderboard
                  SELECT ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY score DESC) AS place,
                         match_time,
                         match_id,
                         player_id,
                         score
                  FROM player_scores
                  LIMIT 2
                "#
            )
            .as_str(),
        ),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_inspect_schema(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        ["inspect", "schema", "player-scores"],
        None,
        Some(["Warning: Dataset schema is not yet available: player-scores"]),
    )
    .await;

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.assert_success_command_execution(
        [
            "inspect",
            "schema",
            "leaderboard",
            "--output-format",
            "parquet",
        ],
        None,
        Some(["Warning: Dataset schema is not yet available: leaderboard"]),
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
            "inspect",
            "schema",
            "player-scores",
            "--output-format",
            "parquet",
        ],
        Some(indoc::indoc!(
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
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        [
            "inspect",
            "schema",
            "leaderboard",
            "--output-format",
            "parquet",
        ],
        None,
        Some(["Warning: Dataset schema is not yet available: leaderboard"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
