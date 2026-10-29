// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::{AddDatasetOptions, KamuCliPuppetExt, RepoAlias};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_repository_aliases_commands(kamu: KamuCliPuppet) {
    kamu.add_dataset(
        odf::DatasetSnapshot {
            name: "foo".try_into().unwrap(),
            kind: odf::DatasetKind::Root,
            metadata: vec![
                odf::metadata::AddPushSource {
                    source_name: odf::metadata::SourceState::DEFAULT_SOURCE_NAME.to_string(),
                    read: odf::metadata::ReadStepNdJson {
                        schema: Some(odf::schema::DataSchema::new(vec![
                            odf::schema::DataField::timestamp_millis_utc("event_time"),
                            odf::schema::DataField::string("foo_string"),
                        ])),
                        ..Default::default()
                    }
                    .into(),
                    preprocess: None,
                    merge: odf::metadata::MergeStrategyLedger {
                        primary_key: vec!["event_time".to_owned(), "foo_string".to_owned()],
                    }
                    .into(),
                }
                .into(),
            ],
        },
        AddDatasetOptions::default(),
    )
    .await;
    let foo = odf::DatasetRef::from(odf::DatasetName::new_unchecked("foo"));

    let pull = repo_alias("Pull", "http://pull.example.com/");
    let shared_pull = repo_alias("Pull", "http://shared.example.com/");
    let push = repo_alias("Push", "http://push.example.com/");
    let shared_push = repo_alias("Push", "http://shared.example.com/");

    for (alias, kind_flag) in [
        (&pull.alias, "--pull"),
        (&shared_pull.alias, "--pull"),
        (&push.alias, "--push"),
        (&shared_push.alias, "--push"),
    ] {
        kamu.execute(["repo", "alias", "add", "foo", alias, kind_flag])
            .await
            .success();
    }

    pretty_assertions::assert_eq!(
        kamu.get_list_of_repo_aliases(&foo).await,
        [
            pull.clone(),
            shared_pull.clone(),
            push.clone(),
            shared_push.clone()
        ]
    );

    // A kind flag removes the alias of that kind only
    kamu.execute(["repo", "alias", "rm", "foo", &shared_push.alias, "--push"])
        .await
        .success();

    pretty_assertions::assert_eq!(
        kamu.get_list_of_repo_aliases(&foo).await,
        [pull.clone(), shared_pull.clone(), push.clone()]
    );

    // Without a kind flag, the alias is removed whatever its kind
    kamu.execute(["repo", "alias", "rm", "foo", &pull.alias])
        .await
        .success();

    pretty_assertions::assert_eq!(
        kamu.get_list_of_repo_aliases(&foo).await,
        [shared_pull, push]
    );

    kamu.execute(["repo", "alias", "rm", "--all", "foo"])
        .await
        .success();

    assert_eq!(kamu.get_list_of_repo_aliases(&foo).await, []);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn repo_alias(kind: &str, alias: &str) -> RepoAlias {
    RepoAlias {
        dataset: "foo".parse().unwrap(),
        kind: kind.to_string(),
        alias: alias.to_string(),
    }
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
        Some([r#"You can either specify repository\(s\) or pass --all"#]),
    )
    .await;

    kamu.assert_failure_command_execution(
        ["repo", "delete"],
        None,
        Some([r#"Specify repository\(s\) or pass --all"#]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
