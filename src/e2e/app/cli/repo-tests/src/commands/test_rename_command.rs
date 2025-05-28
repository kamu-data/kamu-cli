// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR;
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_rename_dataset(kamu: KamuCliPuppet) {
    kamu.assert_failure_command_execution(
        ["rename", "player-scores", "top-player-scores"],
        None,
        Some(["Error: Dataset not found: player-scores"]),
    )
    .await;

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        ["rename", "player-scores", "top-player-scores"],
        None,
        Some(["Dataset renamed"]),
    )
    .await;

    let dataset_names = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(vec!["top-player-scores"], dataset_names);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
