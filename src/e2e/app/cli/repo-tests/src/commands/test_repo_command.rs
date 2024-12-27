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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_repository_pull_aliases_commands(kamu: KamuCliPuppet) {
    kamu.add_dataset(odf::DatasetSnapshot {
        name: "foo".try_into().unwrap(),
        kind: odf::DatasetKind::Root,
        metadata: vec![odf::metadata::AddPushSource {
            source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: odf::metadata::ReadStepNdJson {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "foo_string STRING".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: odf::metadata::MergeStrategyLedger {
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
        .get_list_of_repo_aliases(&odf::DatasetRef::from(odf::DatasetName::new_unchecked(
            "foo",
        )))
        .await;
    pretty_assertions::assert_eq!(dataset_aliases, aliases);

    // Test remove single pull alias
    kamu.execute(["repo", "alias", "rm", "foo", &dataset_aliases[2].alias])
        .await
        .success();

    let aliases = kamu
        .get_list_of_repo_aliases(&odf::DatasetRef::from(odf::DatasetName::new_unchecked(
            "foo",
        )))
        .await;
    pretty_assertions::assert_eq!(dataset_aliases[..2], aliases);

    // Test remove all pull aliases
    kamu.execute(["repo", "alias", "rm", "--all", "foo"])
        .await
        .success();

    let aliases = kamu
        .get_list_of_repo_aliases(&odf::DatasetRef::from(odf::DatasetName::new_unchecked(
            "foo",
        )))
        .await;
    assert!(aliases.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_repository_push_aliases_commands(kamu: KamuCliPuppet) {
    kamu.add_dataset(odf::DatasetSnapshot {
        name: "foo".try_into().unwrap(),
        kind: odf::DatasetKind::Root,
        metadata: vec![odf::metadata::AddPushSource {
            source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
            read: odf::metadata::ReadStepNdJson {
                schema: Some(vec![
                    "event_time TIMESTAMP".to_owned(),
                    "foo_string STRING".to_owned(),
                ]),
                ..Default::default()
            }
            .into(),
            preprocess: None,
            merge: odf::metadata::MergeStrategyLedger {
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
        .get_list_of_repo_aliases(&odf::DatasetRef::from(odf::DatasetName::new_unchecked(
            "foo",
        )))
        .await;
    pretty_assertions::assert_eq!(dataset_aliases, aliases);

    // Test remove single push alias
    kamu.execute(["repo", "alias", "rm", "foo", &dataset_aliases[2].alias])
        .await
        .success();

    let aliases = kamu
        .get_list_of_repo_aliases(&odf::DatasetRef::from(odf::DatasetName::new_unchecked(
            "foo",
        )))
        .await;
    pretty_assertions::assert_eq!(dataset_aliases[..2], aliases);
    // Test remove all push aliases
    kamu.execute(["repo", "alias", "rm", "--all", "foo"])
        .await
        .success();

    let aliases = kamu
        .get_list_of_repo_aliases(&odf::DatasetRef::from(odf::DatasetName::new_unchecked(
            "foo",
        )))
        .await;
    assert!(aliases.is_empty());
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_repo_delete_args_validation(kamu: KamuCliPuppet) {
    kamu.assert_success_command_execution(
        ["repo", "delete", "--all"],
        None,
        Some(["There are no repositories to delete"]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["repo", "delete", "some-repo"],
        None,
        Some(["Error: Repository some-repo does not exist"]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["repo", "delete", "some-repo", "--all"],
        None,
        Some(["You can either specify repository(s) or pass --all"]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["repo", "delete"],
        None,
        Some(["Specify repository(s) or pass --all"]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
