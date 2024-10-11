// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::extensions::{KamuCliPuppetExt, RepoAlias};
use kamu_cli_puppet::KamuCliPuppet;
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
        RepoAlias {
            dataset: "foo".parse().unwrap(),
            kind: "Pull".to_string(),
            alias: "http://pull.example.com/".to_string(),
        },
        RepoAlias {
            dataset: "foo".parse().unwrap(),
            kind: "Pull".to_string(),
            alias: "http://pull.example1.com/".to_string(),
        },
        RepoAlias {
            dataset: "foo".parse().unwrap(),
            kind: "Pull".to_string(),
            alias: "http://pull.example2.com/".to_string(),
        },
    ];

    for repo_alias in &dataset_aliases {
        kamu.execute(["repo", "alias", "add", "foo", &repo_alias.alias, "--pull"])
            .await
            .success();
    }

    let aliases = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert_eq!(aliases, dataset_aliases);

    // Test remove single pull alias
    kamu.execute(["repo", "alias", "rm", "foo", &dataset_aliases[2].alias])
        .await
        .success();

    let aliases = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert_eq!(aliases, dataset_aliases[..2]);

    // Test remove all pull aliases
    kamu.execute(["repo", "alias", "rm", "--all", "foo"])
        .await
        .success();

    let aliases = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert!(aliases.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
        RepoAlias {
            dataset: "foo".parse().unwrap(),
            kind: "Push".to_string(),
            alias: "http://push.example.com/".to_string(),
        },
        RepoAlias {
            dataset: "foo".parse().unwrap(),
            kind: "Push".to_string(),
            alias: "http://push.example1.com/".to_string(),
        },
        RepoAlias {
            dataset: "foo".parse().unwrap(),
            kind: "Push".to_string(),
            alias: "http://push.example2.com/".to_string(),
        },
    ];

    for repo_alias in &dataset_aliases {
        kamu.execute(["repo", "alias", "add", "foo", &repo_alias.alias, "--push"])
            .await
            .success();
    }

    let aliases = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert_eq!(aliases, dataset_aliases);

    // Test remove single push alias
    kamu.execute(["repo", "alias", "rm", "foo", &dataset_aliases[2].alias])
        .await
        .success();

    let aliases = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert_eq!(aliases, dataset_aliases[..2]);
    // Test remove all push aliases
    kamu.execute(["repo", "alias", "rm", "--all", "foo"])
        .await
        .success();

    let aliases = kamu
        .get_list_of_repo_aliases(&DatasetRef::from(DatasetName::new_unchecked("foo")))
        .await;
    assert!(aliases.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
