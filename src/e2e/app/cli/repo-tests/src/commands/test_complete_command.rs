// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::{AddDatasetOptions, KamuCliPuppetExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_subcommand(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu l", 1).await;

    assert_eq!(completions, ["list", "log", "login", "logout"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_config(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu config set engine.runt", 3).await;

    assert_eq!(completions, ["engine.runtime"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_complete_dataset_name(kamu: KamuCliPuppet) {
    kamu.add_dataset(
        odf::DatasetSnapshot {
            name: "foo.bar".try_into().unwrap(),
            kind: odf::DatasetKind::Root,
            metadata: vec![],
        },
        AddDatasetOptions::default(),
    )
    .await;

    let completions = kamu.complete("kamu log", 2).await;

    assert_eq!(completions, ["foo.bar"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Every accepted spelling is offered: canonical schema name and aliases.
pub async fn test_complete_resource_type(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu get", 2).await;

    for expected in [
        "VariableSet",
        "variablesets",
        "vs",
        "SecretSet",
        "secretsets",
        "ss",
        "Storage",
        "storages",
        "st",
    ] {
        assert!(
            completions.contains(&expected.to_string()),
            "missing {expected} in {completions:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `list`/`delete` also accept the `datasets` and `%` targets.
pub async fn test_complete_resource_type_extra_targets(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu list", 2).await;

    for expected in ["dataset", "datasets", "%", "variablesets"] {
        assert!(
            completions.contains(&expected.to_string()),
            "missing {expected} in {completions:?}"
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// The implicit `local` context is offered inside a workspace.
pub async fn test_complete_context_name(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu context use", 3).await;

    assert!(
        completions.contains(&"local".to_string()),
        "missing local in {completions:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Reached through the option's `CONTEXT_NAME` value name, not a positional.
pub async fn test_complete_context_option_value(kamu: KamuCliPuppet) {
    let completions = kamu.complete("kamu list --context", 3).await;

    assert!(
        completions.contains(&"local".to_string()),
        "missing local in {completions:?}"
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// `kamu complete` runs with `command_needs_workspace == false`, so the
/// registries it consults may be absent; it must degrade rather than panic.
pub async fn test_complete_outside_workspace_does_not_crash(kamu: KamuCliPuppet) {
    // Resource types and `local` both need a workspace; extras remain.
    let completions = kamu.complete("kamu list", 2).await;
    assert!(
        !completions.contains(&"variablesets".to_string()),
        "unexpected resource types outside a workspace: {completions:?}"
    );

    let completions = kamu.complete("kamu l", 1).await;
    assert_eq!(completions, ["list", "log", "login", "logout"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
