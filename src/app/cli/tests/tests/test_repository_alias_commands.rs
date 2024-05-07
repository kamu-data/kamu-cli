// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric::*;

use crate::utils::Kamu;

#[test_log::test(tokio::test)]
async fn test_repository_alias_commands() {
    let kamu = Kamu::new_workspace_tmp().await;
    kamu.add_dataset(DatasetSnapshot {
        name: "test".try_into().unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![AddPushSource {
            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: ReadStepNdJson {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "test STRING".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: MergeStrategyLedger {
                primary_key: vec!["event_time".to_owned(), "test".to_owned()],
            }
            .into(),
        }
        .into()],
    })
    .await
    .unwrap();
    let dataset_aliases = vec![
        "http://net.test.com/".to_string(),
        "http://net.test1.com/".to_string(),
    ];

    let cmd_result = kamu
        .execute([
            "repo",
            "alias",
            "add",
            "test",
            dataset_aliases[0].as_str(),
            "--pull",
        ])
        .await;
    assert!(cmd_result.is_ok());
    let cmd_result = kamu
        .execute([
            "repo",
            "alias",
            "add",
            "test",
            dataset_aliases[1].as_str(),
            "--pull",
        ])
        .await;
    assert!(cmd_result.is_ok());

    let (pull_aliases, _push_aliases) = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("test")))
        .await;
    assert_eq!(pull_aliases, dataset_aliases);

    let cmd_result = kamu.execute(["repo", "alias", "rm", "--all", "test"]).await;
    assert!(cmd_result.is_ok());
    let (pull_aliases, _push_aliases) = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("test")))
        .await;
    assert!(pull_aliases.is_empty());
}
