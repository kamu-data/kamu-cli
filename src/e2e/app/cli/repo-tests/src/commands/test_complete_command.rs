// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::extensions::{AddDatasetOptions, KamuCliPuppetExt};
use kamu_cli_puppet::KamuCliPuppet;

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
