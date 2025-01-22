// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;
use odf::metadata::testing::MetadataFactory;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_add_dataset_from_stdin(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: player-scores
            Added 1 dataset\(s\)
            "#
        )]),
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

pub async fn test_add_dataset_with_name(kamu: KamuCliPuppet) {
    // Add from stdio
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin", "--name", "player-scores-1"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: player-scores-1
            Added 1 dataset\(s\)
            "#
        )]),
    )
    .await;

    // Add from a file
    let snapshot_path = kamu.workspace_path().join("player-scores.yaml");

    std::fs::write(
        snapshot_path.clone(),
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    )
    .unwrap();

    kamu.assert_success_command_execution(
        [
            "add",
            "--name",
            "player-scores-2",
            snapshot_path.to_str().unwrap(),
        ],
        None,
        Some([indoc::indoc!(
            r#"
            Added: player-scores-2
            Added 1 dataset\(s\)
            "#
        )]),
    )
    .await;

    let dataset_names = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(vec!["player-scores-1", "player-scores-2"], dataset_names);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_add_dataset_with_replace(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: player-scores
            "#
        )]),
    )
    .await;

    kamu.assert_success_command_execution_with_input(
        ["add", "--stdin"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Skipped: player-scores: Already exists
            Added 0 dataset\(s\)
            "#
        )]),
    )
    .await;

    kamu.assert_success_command_execution_with_input(
        ["--yes", "add", "--stdin", "--replace"],
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
        None,
        Some([indoc::indoc!(
            r#"
            Added: player-scores
            Added 1 dataset\(s\)
            "#
        )]),
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

pub async fn test_add_recursive(kamu: KamuCliPuppet) {
    // Plain manifest
    let snapshot = MetadataFactory::dataset_snapshot().name("plain").build();
    let manifest = odf::serde::yaml::YamlDatasetSnapshotSerializer
        .write_manifest_str(&snapshot)
        .unwrap();
    std::fs::write(kamu.workspace_path().join("plain.yaml"), manifest).unwrap();

    // Manifest with lots of comments
    let snapshot = MetadataFactory::dataset_snapshot()
        .name("commented")
        .build();
    let manifest = odf::serde::yaml::YamlDatasetSnapshotSerializer
        .write_manifest_str(&snapshot)
        .unwrap();
    std::fs::write(
        kamu.workspace_path().join("commented.yaml"),
        format!(
            indoc::indoc! {
                "

                # Some

                # Weird
                #
                # Comment
                {}
                "
            },
            &manifest
        ),
    )
    .unwrap();

    // Non-manifest YAML file
    std::fs::write(
        kamu.workspace_path().join("non-manifest.yaml"),
        indoc::indoc! {
            "
            foo:
              - bar
            "
        },
    )
    .unwrap();

    kamu.assert_success_command_execution(
        [
            "-v",
            "add",
            "--recursive",
            kamu.workspace_path().as_os_str().to_str().unwrap(),
        ],
        None,
        Some([indoc::indoc!(
            r#"
            Added: commented
            Added: plain
            Added 2 dataset\(s\)
            "#
        )]),
    )
    .await;

    let dataset_names = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name.to_string())
        .collect::<Vec<_>>();

    pretty_assertions::assert_eq!(vec!["commented", "plain"], dataset_names);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_add_with_circular_dependency(kamu: KamuCliPuppet) {
    // Plain manifest
    let snapshot = MetadataFactory::dataset_snapshot().name("plain").build();
    let manifest = odf::serde::yaml::YamlDatasetSnapshotSerializer
        .write_manifest_str(&snapshot)
        .unwrap();
    std::fs::write(kamu.workspace_path().join("plain.yaml"), manifest).unwrap();

    // Manifest with lots of comments
    let snapshot = MetadataFactory::dataset_snapshot()
        .name("plain")
        .kind(odf::DatasetKind::Derivative)
        .push_event(
            MetadataFactory::set_transform()
                .inputs_from_refs(["plain"])
                .build(),
        )
        .build();
    let manifest = odf::serde::yaml::YamlDatasetSnapshotSerializer
        .write_manifest_str(&snapshot)
        .unwrap();
    std::fs::write(
        kamu.workspace_path().join("commented.yaml"),
        format!(
            indoc::indoc! {
                "

                # Some

                # Weird
                #
                # Comment
                {}
                "
            },
            &manifest
        ),
    )
    .unwrap();

    kamu.assert_success_command_execution(
        ["-v", "add", "plain.yaml", "commented.yaml"],
        None,
        Some([indoc::indoc!(
            r#"
            Added: plain
            Skipped: plain: Already exists
            Added 1 dataset\(s\)
            "#
        )]),
    )
    .await;

    let dataset_names = kamu
        .list_datasets()
        .await
        .into_iter()
        .map(|dataset| dataset.name)
        .collect::<Vec<_>>();

    assert_eq!(dataset_names, ["plain"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
