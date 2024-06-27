// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli::testing::Kamu;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[test_log::test(tokio::test)]
async fn test_complete_subcommand() {
    let kamu = Kamu::new(".");
    let completions = kamu.complete("kamu l", 1).await.unwrap();
    assert_eq!(completions, ["list", "log", "login", "logout"]);
}

#[test_log::test(tokio::test)]
async fn test_complete_config() {
    let kamu = Kamu::new(".");

    let completions = kamu
        .complete("kamu config set engine.runt", 3)
        .await
        .unwrap();
    assert_eq!(completions, ["engine.runtime"]);
}

#[test_log::test(tokio::test)]
async fn test_complete_dataset_name() {
    let kamu = Kamu::new_workspace_tmp().await;

    kamu.add_dataset(DatasetSnapshot {
        name: "foo.bar".try_into().unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![],
    })
    .await
    .unwrap();

    let completions = kamu.complete("kamu log", 2).await.unwrap();
    assert_eq!(completions, ["foo.bar"]);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
