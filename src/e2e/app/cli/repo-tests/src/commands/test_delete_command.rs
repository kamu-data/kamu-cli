// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::*;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset(kamu: KamuCliPuppet) {
    kamu.assert_failure_command_execution(
        ["delete", "player-scores"],
        None,
        Some(["Error: Dataset not found: player-scores"]),
    )
    .await;

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        ["--yes", "delete", "player-scores"],
        None,
        Some([r#"Deleted 1 dataset\(s\)"#]),
    )
    .await;

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
            .map(|dataset| dataset.name.to_string())
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(
            vec!["another-root", "leaderboard", "player-scores"],
            dataset_names,
        );
    }

    kamu.assert_success_command_execution(
        ["--yes", "delete", "player-scores", "--recursive"],
        None,
        Some([r#"Deleted 2 dataset\(s\)"#]),
    )
    .await;

    {
        let dataset_names = kamu
            .list_datasets()
            .await
            .into_iter()
            .map(|dataset| dataset.name.to_string())
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(vec!["another-root"], dataset_names);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_all(kamu: KamuCliPuppet) {
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
            .map(|dataset| dataset.name.to_string())
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(
            vec!["another-root", "leaderboard", "player-scores"],
            dataset_names
        );
    }

    kamu.assert_success_command_execution(
        ["--yes", "delete", "--all"],
        None,
        Some([r#"Deleted 3 dataset\(s\)"#]),
    )
    .await;

    {
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
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_warning(mut kamu_node_api_client: KamuApiServerClient) {
    let kamu = KamuCliPuppet::new_workspace_tmp_multi_tenant().await;

    let ds_name_str = "player-scores";
    let ds_name = odf::DatasetName::new_unchecked(ds_name_str);

    kamu.execute_with_input(
        ["add", "--stdin", "--name", ds_name_str],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();

    let repo_path = kamu.workspace_path().join("repo");
    let file_repo_url = Url::from_file_path(&repo_path).unwrap();
    kamu.execute(["repo", "add", "file-repo", file_repo_url.as_str()])
        .await
        .success();

    let kamu_node_url = kamu_node_api_client.get_base_url().clone();
    let (token, _) = kamu_node_api_client.auth().login_as_e2e_user().await;

    kamu.assert_success_command_execution(
        [
            "login",
            "--access-token",
            token.as_str(),
            "--repo-name",
            "remote-repo",
            kamu_node_url.as_str(),
        ],
        None,
        Some([format!("Login successful: {kamu_node_url}").as_str()]),
    )
    .await;

    kamu.execute_with_input(
        ["ingest", ds_name_str, "--stdin", "--source-name", "default"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await
    .success();

    kamu.execute(["push", ds_name_str, "--to", "remote-repo/pull-1"])
        .await
        .success();

    kamu.execute(["push", ds_name_str, "--to", "file-repo/removed"])
        .await
        .success();

    let rm_path = kamu.workspace_path().join("repo").join("removed");
    std::fs::remove_dir_all(rm_path).unwrap();

    kamu.execute_with_input(
        ["ingest", ds_name_str, "--stdin", "--source-name", "default"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await
    .success();

    kamu.assert_success_command_execution(
        ["push", ds_name_str, "--to", "file-repo/pull-1-2"],
        None,
        Some([r#"1 dataset\(s\) pushed"#]),
    )
    .await;

    // Removing last data portion
    let blocks = kamu.list_blocks(&ds_name).await;
    let head = &blocks[1];
    let head_hash = head.block_hash.as_multibase().to_stack_string();
    kamu.assert_success_command_execution(
        ["--yes", "reset", ds_name_str, head_hash.as_str()],
        None,
        Some(["Dataset was reset"]),
    )
    .await;

    kamu.execute_with_input(
        ["ingest", ds_name_str, "--stdin", "--source-name", "default"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3,
    )
    .await
    .success();

    kamu.execute_with_input(
        ["ingest", ds_name_str, "--stdin", "--source-name", "default"],
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_4,
    )
    .await
    .success();

    kamu.assert_success_command_execution(
        ["push", ds_name_str, "--to", "file-repo/pull-1-3-4"],
        None,
        Some([r#"1 dataset\(s\) pushed"#]),
    )
    .await;

    // Removing last data portion
    let blocks = kamu.list_blocks(&ds_name).await;
    let head = &blocks[1];
    let head_hash = head.block_hash.as_multibase().to_stack_string();
    kamu.assert_success_command_execution(
        ["--yes", "reset", ds_name_str, head_hash.as_str()],
        None,
        Some(["Dataset was reset"]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["push", ds_name_str, "--to", "file-repo/pull-1-3"],
        None,
        Some([r#"1 dataset\(s\) pushed"#]),
    )
    .await;

    let pull_1_url = kamu_node_api_client
        .get_odf_node_url()
        .join("e2e-user/")
        .unwrap()
        .join("pull-1")
        .unwrap();
    let pull_1_2_url = Url::from_file_path(repo_path.join("pull-1-2")).unwrap();
    let pull_1_3_4_url = Url::from_file_path(repo_path.join("pull-1-3-4")).unwrap();
    let removed_url = Url::from_file_path(repo_path.join("removed")).unwrap();

    use regex::escape as e;

    kamu.assert_success_command_execution(
        ["--yes", "delete", "--all"],
        None,
        Some([
            r#"player-scores dataset is out of sync with remote\(s\):"#,
            &format!(r#" - ahead of '{}'"#, e(pull_1_url.as_str())),
            &format!(r#" - diverged from '{}'"#, e(pull_1_2_url.as_str())),
            &format!(r#" - behind '{}'"#, e(pull_1_3_4_url.as_str())),
            &format!(
                " - could not check state of '{}'. Error: Remote dataset not found",
                e(removed_url.as_str())
            ),
        ]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_args_validation(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution(
        ["delete", "--all"],
        None,
        Some(["There are no datasets matching the pattern"]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["delete", "player-scores", "--all"],
        None,
        Some([r#"You can either specify dataset\(s\) or pass --all"#]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["delete"],
        None,
        Some([r#"Specify dataset\(s\) or pass --all"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
