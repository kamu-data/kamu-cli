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
use pretty_assertions::assert_eq;

use crate::resources::{ResourceCtx, fixtures};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Scenario: deleting a dataset retracts the legacy-config-target-dataset label
//
// Pins both treatments against the real binary: an auto-managed
// `legacy-vars-<multibase>` set is deleted outright, while a user-authored set
// keeps its spec and loses only the dangling label.
//
// The only test that proves the outbox really delivers `Deleted` to the
// configuration consumer — the unit tests call `consume_message` directly, so
// they cannot catch missing DI or dispatcher registration. Local-only, since
// the dataset lives in the CLI's own workspace.
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_resources_dataset_deletion_cleans_up_legacy_config(kamu: KamuCliPuppet) {
    // Local-only: the dataset lives in this workspace, so the resources that
    // point at it must be evaluated on the same node.
    let ctx = ResourceCtx::Local(kamu);

    let dataset_name = "player-scores";

    ctx.kamu()
        .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    // The real DID, which is what the label must carry.
    let datasets = ctx.kamu().list_datasets().await;
    let dataset_did = datasets
        .iter()
        .find(|d| d.name.as_str() == dataset_name)
        .expect("player-scores must exist after `add`")
        .id
        .as_did_str()
        .to_string();

    // An auto-managed set, named exactly the way the backfill migration and the
    // env-var mutation adapter name theirs.
    let auto_managed_name = format!(
        "legacy-vars-{}",
        dataset_did.strip_prefix("did:odf:").unwrap()
    );

    for name in [auto_managed_name.as_str(), "my-vars"] {
        let manifest = fixtures::variable_set_manifest_with_environment_label(
            name,
            &dataset_did,
            fixtures::LEGACY_CONFIG_TARGET_DATASET_LABEL_NAME,
        );
        ctx.assert_success_with_stdin(["apply", "--stdin"], &manifest, None)
            .await;
    }

    // Precondition: both are present and both resolve through the label.
    let mut before = ctx
        .list_names_with_labels(
            "vs",
            &[&format!(
                "{}={dataset_did}",
                fixtures::LEGACY_CONFIG_TARGET_DATASET_LABEL_NAME
            )],
        )
        .await;
    before.sort();
    assert_eq!(
        before,
        vec![auto_managed_name.clone(), "my-vars".to_string()],
        "both sets must be labelled as targeting the dataset before it is deleted"
    );

    ctx.kamu()
        .assert_success_command_execution(
            ["delete", dataset_name, "--force"],
            None,
            Some([r#"Deleted: player-scores"#]),
        )
        .await;

    // The auto-managed set is gone; the user-authored one survives.
    assert_eq!(
        ctx.list_names("vs").await,
        vec!["my-vars".to_string()],
        "the auto-managed set must be deleted, the user-authored one kept"
    );

    // And nothing points at the dead dataset any more.
    assert_eq!(
        ctx.list_names_with_labels(
            "vs",
            &[&format!(
                "{}={dataset_did}",
                fixtures::LEGACY_CONFIG_TARGET_DATASET_LABEL_NAME
            )],
        )
        .await,
        Vec::<String>::new(),
        "the dangling label must be retracted from the surviving set"
    );

    // The survivor keeps everything else: spec and unrelated metadata.
    let view = ctx.get_one(["get", "vs", "my-vars"]).await;
    assert_eq!(
        view.label(fixtures::LEGACY_CONFIG_TARGET_DATASET_LABEL_SCHEMA),
        None,
        "only the dangling label is removed"
    );
    assert_eq!(
        view.variable("MESSAGE"),
        Some("value"),
        "the user's spec must survive untouched"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
