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
use opendatafabric as odf;
use url::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

const DATASET_DEL_WARN_SNAPSHOT_STR: &str = indoc::indoc!(
    r#"
    kind: DatasetSnapshot
    version: 1
    content:
      name: test.delete.warning
      kind: Root
      metadata:
        - kind: AddPushSource
          sourceName: default
          read:
            kind: Csv
            header: true
            separator: ','
          merge:
            kind: Append
    "#
);

const DATA_DEL_WARN_1: &str = indoc::indoc!(
    r#"
    label,num
    A,1000
    "#
);

const DATA_DEL_WARN_2: &str = indoc::indoc!(
    r#"
    label,num
    B,2000
    "#
);
const DATA_DEL_WARN_3: &str = indoc::indoc!(
    r#"
    label,num
    C,3000
    "#
);
const DATA_DEL_WARN_4: &str = indoc::indoc!(
    r#"
    label,num
    D,4000
    "#
);

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
        Some(["Deleted 1 dataset(s)"]),
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
        Some(["Deleted 2 dataset(s)"]),
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
        Some(["Deleted 3 dataset(s)"]),
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

pub async fn test_delete_warning(kamu: KamuCliPuppet) {
    let ds_name: odf::DatasetName = odf::DatasetName::new_unchecked("test.delete.warning");

    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_DEL_WARN_SNAPSHOT_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: test.delete.warning
            Added 1 dataset(s)
            "#
        )]),
    )
    .await;

    // TODO: use remote http repo as well
    let repo_path = kamu.workspace_path().join("repo");
    let file_repo_url = Url::from_file_path(&repo_path).unwrap();
    kamu.assert_success_command_execution(
        ["repo", "add", "file-repo", file_repo_url.as_str()],
        None,
        Some([indoc::indoc!(
            r#"
                Added: file-repo
            "#
        )]),
    )
    .await;

    kamu.execute_with_input(
        ["ingest", "test.delete.warning", "--stdin"],
        DATA_DEL_WARN_1,
    )
    .await
    .success();

    kamu.assert_success_command_execution(
        ["push", "test.delete.warning", "--to", "file-repo/pull-1"],
        None,
        Some([indoc::indoc!(
            r#"
                1 dataset(s) pushed
            "#
        )]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["push", "test.delete.warning", "--to", "file-repo/removed"],
        None,
        Some([indoc::indoc!(
            r#"
                1 dataset(s) pushed
            "#
        )]),
    )
    .await;
    let rm_path = kamu.workspace_path().join("repo").join("removed");
    std::fs::remove_dir_all(rm_path).unwrap();

    kamu.execute_with_input(
        ["ingest", "test.delete.warning", "--stdin"],
        DATA_DEL_WARN_2,
    )
    .await
    .success();

    kamu.assert_success_command_execution(
        ["push", "test.delete.warning", "--to", "file-repo/pull-1-2"],
        None,
        Some([indoc::indoc!(
            r#"
                1 dataset(s) pushed
            "#
        )]),
    )
    .await;

    // Removing last data portion
    let blocks = kamu.list_blocks(&ds_name).await;
    let head = &blocks[1];
    let head_hash = head.block_hash.as_multibase().to_stack_string();
    kamu.assert_success_command_execution(
        ["--yes", "reset", "test.delete.warning", head_hash.as_str()],
        None,
        Some(["Dataset was reset"]),
    )
    .await;

    kamu.execute_with_input(
        ["ingest", "test.delete.warning", "--stdin"],
        DATA_DEL_WARN_3,
    )
    .await
    .success();

    kamu.execute_with_input(
        ["ingest", "test.delete.warning", "--stdin"],
        DATA_DEL_WARN_4,
    )
    .await
    .success();

    kamu.assert_success_command_execution(
        [
            "push",
            "test.delete.warning",
            "--to",
            "file-repo/pull-1-3-4",
        ],
        None,
        Some([indoc::indoc!(
            r#"
                1 dataset(s) pushed
            "#
        )]),
    )
    .await;

    // Removing last data portion
    let blocks = kamu.list_blocks(&ds_name).await;
    let head = &blocks[1];
    let head_hash = head.block_hash.as_multibase().to_stack_string();
    kamu.assert_success_command_execution(
        ["--yes", "reset", "test.delete.warning", head_hash.as_str()],
        None,
        Some(["Dataset was reset"]),
    )
    .await;

    kamu.assert_success_command_execution(
        ["push", "test.delete.warning", "--to", "file-repo/pull-1-3"],
        None,
        Some([indoc::indoc!(
            r#"
                1 dataset(s) pushed
            "#
        )]),
    )
    .await;

    let pull_1_url = Url::from_file_path(repo_path.join("pull-1")).unwrap();
    let pull_1_2_url = Url::from_file_path(repo_path.join("pull-1-2")).unwrap();
    let pull_1_3_4_url = Url::from_file_path(repo_path.join("pull-1-3-4")).unwrap();
    let removed_url = Url::from_file_path(repo_path.join("removed")).unwrap();
    let expected_out = indoc::formatdoc!(
        r#"
            test.delete.warning dataset is out of sync with remote(s):
             - ahead of '{pull_1_url}'
             - diverged from '{pull_1_2_url}'
             - behind '{pull_1_3_4_url}'
             - could not check state of '{removed_url}'. Error: Remote dataset not found
        "#
    );
    kamu.assert_success_command_execution(
        ["--yes", "delete", "--all"],
        None,
        Some([expected_out.as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
