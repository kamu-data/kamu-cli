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
use opendatafabric::{DatasetName, EnumWithVariants, SetTransform};

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

    {
        let assert = kamu
            .execute(["inspect", "lineage", "--output-format", "shell"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            stdout,
            indoc::indoc!(
                r#"
                leaderboard: Derivative
                └── player-scores: Root
                player-scores: Root
                "#
            )
        );
    }
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
            if dataset.name == DatasetName::new_unchecked("player-scores") {
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

    let leaderboard_transform_block_hash = kamu
        .list_blocks(&DatasetName::new_unchecked("leaderboard"))
        .await
        .into_iter()
        .find_map(|block| {
            if block.block.event.as_variant::<SetTransform>().is_some() {
                Some(block.block_hash)
            } else {
                None
            }
        })
        .unwrap();

    {
        let assert = kamu
            .execute(["inspect", "query", "player-scores"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(stdout, "");
    }
    {
        let assert = kamu
            .execute(["inspect", "query", "leaderboard"])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            stdout,
            indoc::formatdoc!(
                r#"
                Transform: {leaderboard_transform_block_hash}
                As Of: 2050-01-02T03:04:05Z
                Inputs:
                  player_scores  {player_scores_dataset_id}
                Engine: risingwave (None)
                Query: leaderboard
                  create materialized view leaderboard as
                  select
                    *
                  from (
                    select
                      row_number() over (partition by 1 order by score desc) as place,
                      match_time,
                      match_id,
                      player_id,
                      score
                    from player_scores
                  )
                  where place <= 2
                Query: leaderboard
                  select * from leaderboard
                "#
            )
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
