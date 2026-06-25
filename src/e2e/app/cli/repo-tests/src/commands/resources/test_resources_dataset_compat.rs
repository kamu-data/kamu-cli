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
// Scenario: legacy dataset compatibility regression (QA scenario 16)
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_dataset_compat(kamu: KamuCliPuppet) {
    let dataset_name = "player-scores";

    add_player_scores_dataset(&kamu).await;

    assert_stdout_contains(&kamu, ["list"], dataset_name).await;
    assert_stdout_contains(&kamu, ["list", "datasets"], dataset_name).await;
    // `list all` is the resource surface and succeeds, but must not leak the
    // dataset into it. With no resources present it legitimately renders empty
    // (`[]`), so we only assert the dataset is absent.
    assert_stdout_excludes(&kamu, ["list", "all"], dataset_name).await;

    kamu.assert_success_command_execution(
        ["delete", dataset_name, "--force"],
        None,
        Some([
            r#"Deleted: player-scores"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    add_player_scores_dataset(&kamu).await;

    kamu.assert_success_command_execution(
        ["delete", "datasets", dataset_name, "--force"],
        None,
        Some([
            r#"Deleted: player-scores"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["get", "dataset/player-scores"],
        None,
        Some([r#"Unsupported get target 'dataset'"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn add_player_scores_dataset(kamu: &KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_stdout_contains<const N: usize>(
    kamu: &KamuCliPuppet,
    args: [&str; N],
    expected: &str,
) {
    let result = kamu.execute(args).await.success();
    let stdout = std::str::from_utf8(&result.get_output().stdout).unwrap();

    assert!(
        stdout.contains(expected),
        "`{}` should mention {expected}, got:\n{stdout}",
        args.join(" ")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_stdout_excludes<const N: usize>(
    kamu: &KamuCliPuppet,
    args: [&str; N],
    unexpected: &str,
) {
    let result = kamu.execute(args).await.success();
    let stdout = std::str::from_utf8(&result.get_output().stdout).unwrap();

    assert!(
        !stdout.contains(unexpected),
        "`{}` should not mention {unexpected}, got:\n{stdout}",
        args.join(" ")
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
