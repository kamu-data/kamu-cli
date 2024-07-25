// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet_ext::KamuCliPuppetExt;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_repository_pull_aliases_commands(kamu: KamuCliPuppet) {
    kamu.add_dataset(DatasetSnapshot {
        name: "foo".try_into().unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![AddPushSource {
            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: ReadStepNdJson {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "foo_string STRING".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: MergeStrategyLedger {
                primary_key: vec!["event_time".to_owned(), "foo_string".to_owned()],
            }
            .into(),
        }
        .into()],
    })
    .await;

    let dataset_aliases = vec![
        "http://pull.example.com/".to_string(),
        "http://pull.example1.com/".to_string(),
        "http://pull.example2.com/".to_string(),
    ];

    for dataset_alias in &dataset_aliases {
        kamu.execute(["repo", "alias", "add", "foo", dataset_alias, "--pull"])
            .await
            .success();
    }

    let (pull_aliases, _push_aliases) = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert_eq!(pull_aliases, dataset_aliases);

    // Test remove single pull alias
    kamu.execute(["repo", "alias", "rm", "foo", dataset_aliases[2].as_str()])
        .await
        .success();

    let (pull_aliases, _push_aliases) = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert_eq!(pull_aliases, dataset_aliases[..2]);

    // Test remove all pull aliases
    kamu.execute(["repo", "alias", "rm", "--all", "foo"])
        .await
        .success();

    let (pull_aliases, _push_aliases) = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert!(pull_aliases.is_empty());
}

pub async fn test_repository_push_aliases_commands(kamu: KamuCliPuppet) {
    kamu.add_dataset(DatasetSnapshot {
        name: "foo".try_into().unwrap(),
        kind: DatasetKind::Root,
        metadata: vec![AddPushSource {
            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: ReadStepNdJson {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "foo_string STRING".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: MergeStrategyLedger {
                primary_key: vec!["event_time".to_owned(), "foo_string".to_owned()],
            }
            .into(),
        }
        .into()],
    })
    .await;

    let dataset_aliases = vec![
        "http://push.example.com/".to_string(),
        "http://push.example1.com/".to_string(),
        "http://push.example2.com/".to_string(),
    ];

    for dataset_alias in &dataset_aliases {
        kamu.execute(["repo", "alias", "add", "foo", dataset_alias, "--push"])
            .await
            .success();
    }

    let (_pull_aliases, push_aliases) = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert_eq!(push_aliases, dataset_aliases);

    // Test remove single push alias
    kamu.execute(["repo", "alias", "rm", "foo", dataset_aliases[2].as_str()])
        .await
        .success();

    let (_pull_aliases, push_aliases) = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert_eq!(push_aliases, dataset_aliases[..2]);
    // Test remove all push aliases
    kamu.execute(["repo", "alias", "rm", "--all", "foo"])
        .await
        .success();

    let (_pull_aliases, push_aliases) = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert!(push_aliases.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
