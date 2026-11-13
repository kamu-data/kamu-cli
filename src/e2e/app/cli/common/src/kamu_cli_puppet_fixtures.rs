// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

use crate::{
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    city_population_root_snapshot,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Adds the `player-scores` root with its first chunk of data, and the
/// `leaderboard` derivative of it, not yet pulled
pub async fn add_player_scores_with_derivative(kamu: &KamuCliPuppet) {
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
}

/// Adds a root dataset that appends the workspace's `data-*.csv` files of
/// cities and their populations
pub async fn add_city_population_files_dataset(kamu: &KamuCliPuppet, dataset_name: &str) {
    let snapshot = city_population_root_snapshot(
        dataset_name,
        &format!(
            r#"{{ kind: FilesGlob, path: "{}/data-*.csv" }}"#,
            kamu.workspace_path().display()
        ),
    );
    kamu.execute_with_input(["add", "--stdin"], snapshot)
        .await
        .success();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
