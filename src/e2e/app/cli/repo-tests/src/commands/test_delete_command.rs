// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::*;
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::{AddDatasetOptions, KamuCliPuppetExt};
use odf::metadata::testing::MetadataFactory;
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
        Some([
            r#"Deleted: player-scores"#,
            r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#,
        ]),
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
        Some([
            r#"Deleted: leaderboard"#,
            r#"Deleted: player-scores"#,
            r#"Summary 2 item\(s\): 2 deleted, 0 ignored, 0 failed"#,
        ]),
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

pub async fn test_delete_dataset_dry_run(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        ["delete", "player-scores", "--dry-run"],
        None,
        Some([
            r#"Would delete: player-scores"#,
            r#"Summary 1 item\(s\): 1 would delete, 0 ignored, 0 failed"#,
        ]),
    )
    .await;

    let dataset_names = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(vec!["player-scores"], dataset_names);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_ignore_not_found(kamu: KamuCliPuppet) {
    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.assert_success_command_execution(
        [
            "--yes",
            "delete",
            "player-scores",
            "missing-dataset",
            "--ignore-not-found",
        ],
        None,
        Some([
            r#"Ignored: missing-dataset"#,
            r#"Deleted: player-scores"#,
            r#"Summary 2 item\(s\): 1 deleted, 1 ignored, 0 failed"#,
        ]),
    )
    .await;

    let dataset_names = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(Vec::<String>::new(), dataset_names);
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
        Some([r#"Summary 3 item\(s\): 3 deleted, 0 ignored, 0 failed"#]),
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

pub async fn test_delete_dataset_all_respects_current_account(mut kamu: KamuCliPuppet) {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    kamu.create_account(&alice).await;
    kamu.create_account(&bob).await;

    kamu.set_account(Some(alice.clone()));
    kamu.execute_with_input(
        ["add", "--stdin", "--name", "alice-root"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.set_account(Some(bob.clone()));
    kamu.execute_with_input(
        ["add", "--stdin", "--name", "bob-root"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.set_account(Some(alice));
    kamu.assert_success_command_execution(
        ["--yes", "delete", "--all"],
        None,
        Some([r#"Summary 1 item\(s\): 1 deleted, 0 ignored, 0 failed"#]),
    )
    .await;

    let alice_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(Vec::<String>::new(), alice_datasets);

    kamu.set_account(Some(bob));
    let bob_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(vec!["bob-root"], bob_datasets);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_rejects_other_users_dataset(mut kamu: KamuCliPuppet) {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    kamu.create_account(&alice).await;
    kamu.create_account(&bob).await;

    kamu.set_account(Some(bob.clone()));
    kamu.execute_with_input(
        ["add", "--stdin", "--name", "bob-root"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .await
    .success();

    kamu.set_account(Some(alice));
    kamu.assert_failure_command_execution(
        ["delete", "bob/bob-root"],
        None,
        Some([
            r#"Some selected dataset\(s\) cannot be deleted"#,
            r#"User has no 'own' permission in dataset 'bob/bob-root'"#,
        ]),
    )
    .await;

    kamu.set_account(Some(bob));
    let bob_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(vec!["bob-root"], bob_datasets);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_recursive_rejects_foreign_downstream(mut kamu: KamuCliPuppet) {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    kamu.create_account(&alice).await;
    kamu.create_account(&bob).await;

    use odf::metadata::testing::alias;

    let alice_root_alias = alias(&alice, &"alice-root");
    let bob_derivative_alias = alias(&bob, &"bob-derivative");

    kamu.set_account(Some(alice.clone()));
    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(alice_root_alias.dataset_name.as_str())
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.set_account(Some(bob.clone()));
    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(bob_derivative_alias.dataset_name.as_str())
            .kind(odf::DatasetKind::Derivative)
            .push_event(
                MetadataFactory::set_transform()
                    .inputs_from_refs(vec![alice_root_alias.clone()])
                    .build(),
            )
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.set_account(Some(alice));
    kamu.assert_failure_command_execution(
        ["delete", "alice-root", "--recursive"],
        None,
        Some([r#"Recursive delete would affect 1 downstream dataset\(s\) you cannot delete"#]),
    )
    .await;

    let alice_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();
    pretty_assertions::assert_eq!(vec!["alice-root"], alice_datasets);

    kamu.set_account(Some(bob));
    let bob_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();
    pretty_assertions::assert_eq!(vec!["bob-derivative"], bob_datasets);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_recursive_force_orphans_foreign_downstream(
    mut kamu: KamuCliPuppet,
) {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    kamu.create_account(&alice).await;
    kamu.create_account(&bob).await;

    use odf::metadata::testing::alias;

    let alice_root_alias = alias(&alice, &"alice-root");
    let bob_derivative_alias = alias(&bob, &"bob-derivative");

    kamu.set_account(Some(alice.clone()));
    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(alice_root_alias.dataset_name.as_str())
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.set_account(Some(bob.clone()));
    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(bob_derivative_alias.dataset_name.as_str())
            .kind(odf::DatasetKind::Derivative)
            .push_event(
                MetadataFactory::set_transform()
                    .inputs_from_refs(vec![alice_root_alias.clone()])
                    .build(),
            )
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.set_account(Some(alice));
    kamu.assert_success_command_execution(
        ["delete", "alice-root", "--recursive", "--force"],
        None,
        Some([
            r#"Left behind: 1 inaccessible downstream dataset\(s\)"#,
            r#"Deleted: alice/alice-root"#,
            r#"Summary 2 item\(s\): 1 deleted, 1 ignored, 0 failed"#,
        ]),
    )
    .await;

    let alice_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();
    pretty_assertions::assert_eq!(Vec::<String>::new(), alice_datasets);

    kamu.set_account(Some(bob));
    let bob_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();
    pretty_assertions::assert_eq!(vec!["bob-derivative"], bob_datasets);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_delete_dataset_recursive_force_dry_run_previews_foreign_downstream_orphan(
    mut kamu: KamuCliPuppet,
) {
    let alice = odf::AccountName::new_unchecked("alice");
    let bob = odf::AccountName::new_unchecked("bob");

    kamu.create_account(&alice).await;
    kamu.create_account(&bob).await;

    use odf::metadata::testing::alias;

    let alice_root_alias = alias(&alice, &"alice-root");
    let bob_derivative_alias = alias(&bob, &"bob-derivative");

    kamu.set_account(Some(alice.clone()));
    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(alice_root_alias.dataset_name.as_str())
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.set_account(Some(bob.clone()));
    kamu.add_dataset(
        MetadataFactory::dataset_snapshot()
            .name(bob_derivative_alias.dataset_name.as_str())
            .kind(odf::DatasetKind::Derivative)
            .push_event(
                MetadataFactory::set_transform()
                    .inputs_from_refs(vec![alice_root_alias.clone()])
                    .build(),
            )
            .build(),
        AddDatasetOptions::builder()
            .visibility(odf::DatasetVisibility::Public)
            .build(),
    )
    .await;

    kamu.set_account(Some(alice.clone()));
    kamu.assert_success_command_execution(
        [
            "delete",
            "alice-root",
            "--recursive",
            "--force",
            "--dry-run",
        ],
        None,
        Some([
            r#"Would leave behind: 1 inaccessible downstream dataset\(s\)"#,
            r#"Would delete: alice/alice-root"#,
            r#"Summary 2 item\(s\): 1 would delete, 1 ignored, 0 failed"#,
        ]),
    )
    .await;

    let alice_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();
    pretty_assertions::assert_eq!(vec!["alice-root"], alice_datasets);

    kamu.set_account(Some(bob));
    let bob_datasets = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();
    pretty_assertions::assert_eq!(vec!["bob-derivative"], bob_datasets);
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
    let token = kamu_node_api_client.auth().login_as_e2e_user().await;

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
