// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset(kamu: KamuCliPuppet) {
    {
        let assert = kamu.execute(["delete", "player-scores"]).await.failure();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Error: Dataset not found: player-scores"),
            "Unexpected output:\n{stderr}",
        );
    }

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    {
        let assert = kamu
            .execute(["--yes", "delete", "player-scores"])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Deleted 1 dataset(s)"),
            "Unexpected output:\n{stderr}",
        );
    }

    let dataset_names = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name)
        .collect::<Vec<_>>();

    assert!(
        dataset_names.is_empty(),
        "Unexpected dataset names: {dataset_names:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_recursive(kamu: KamuCliPuppet) {
    // 1. Root
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    // 2. Derivative (from 1.)
    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    // 3. One more root
    kamu.execute_with_input(
        ["add", "--stdin", "--name", "another-root"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();

    {
        let dataset_names = kamu
            .list_datasets()
            .await
            .into_iter()
            .map(|dataset| dataset.name)
            .collect::<Vec<_>>();

        assert_eq!(
            dataset_names,
            ["another-root", "leaderboard", "player-scores"]
        );
    }
    {
        let assert = kamu
            .execute(["--yes", "delete", "player-scores", "--recursive"])
            .await
            .success();

        let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

        assert!(
            stderr.contains("Deleted 2 dataset(s)"),
            "Unexpected output:\n{stderr}",
        );
    }
    {
        let dataset_names = kamu
            .list_datasets()
            .await
            .into_iter()
            .map(|dataset| dataset.name)
            .collect::<Vec<_>>();

        assert_eq!(dataset_names, ["another-root"]);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
