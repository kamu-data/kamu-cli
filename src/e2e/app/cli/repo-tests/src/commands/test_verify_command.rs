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
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    leaderboard_set_transform_event,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

use crate::utils::{commit_event, dataset_id, set_private};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_verify_regular_dataset(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    kamu.assert_success_command_execution(
        ["verify", dataset_name.as_str()],
        None,
        Some([r#"1 dataset\(s\) are valid"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_verify_recursive(kamu: KamuCliPuppet) {
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

    kamu.execute(["pull", dataset_derivative_name.as_str()])
        .await
        .success();

    // Call verify without recursive flag
    kamu.assert_success_command_execution(
        ["verify", dataset_derivative_name.as_str()],
        None,
        Some([r#"1 dataset\(s\) are valid"#]),
    )
    .await;

    // Call verify wit recursive flag
    kamu.assert_success_command_execution(
        ["verify", dataset_derivative_name.as_str(), "--recursive"],
        None,
        Some([r#"2 dataset\(s\) are valid"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_verify_integrity(kamu: KamuCliPuppet) {
    let dataset_name = DATASET_ROOT_PLAYER_NAME.clone();

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    kamu.assert_success_command_execution(
        ["verify", dataset_name.as_str(), "--integrity"],
        None,
        Some([r#"1 dataset\(s\) are valid"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_verify_derivative_after_query_change(kamu: KamuCliPuppet) {
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
        &DATASET_ROOT_PLAYER_NAME,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;
    kamu.execute(["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()])
        .await
        .success();

    commit_event(
        &kamu,
        &DATASET_DERIVATIVE_LEADERBOARD_NAME,
        &leaderboard_set_transform_event(
            &dataset_id(&kamu, &DATASET_ROOT_PLAYER_NAME).await,
            "SELECT ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY score ASC) AS place, match_time, \
             match_id, player_id, score FROM player_scores",
        ),
    )
    .await;
    kamu.ingest_data(
        &DATASET_ROOT_PLAYER_NAME,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;
    kamu.execute(["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()])
        .await
        .success();

    // Each block is replayed with the query that produced it
    kamu.assert_success_command_execution(
        ["verify", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()],
        None,
        Some([r#"1 dataset\(s\) are valid"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_verify_replay_denied_when_input_became_unreadable(mut kamu: KamuCliPuppet) {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    kamu.create_account(&alice).await;
    kamu.create_account(&bob).await;

    kamu.set_account(Some(bob.clone()));
    kamu.execute_with_input(
        ["add", "--stdin", "--visibility", "public"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();
    kamu.ingest_data(
        &DATASET_ROOT_PLAYER_NAME,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    kamu.set_account(Some(alice.clone()));
    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR
            .replace("datasetRef: player-scores", "datasetRef: bob/player-scores"),
    )
    .await
    .success();
    kamu.execute(["pull", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()])
        .await
        .success();
    kamu.assert_success_command_execution(
        ["verify", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()],
        None,
        Some([r#"1 dataset\(s\) are valid"#]),
    )
    .await;

    kamu.set_account(Some(bob));
    set_private(&kamu, &DATASET_ROOT_PLAYER_NAME).await;

    kamu.set_account(Some(alice));
    kamu.assert_failure_command_execution(
        ["verify", DATASET_DERIVATIVE_LEADERBOARD_NAME.as_str()],
        None,
        Some([r#"User has no 'read' permission in dataset 'bob/player-scores'"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
