// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use core::panic;

use kamu::testing::MetadataFactory;
use kamu_cli_e2e_common::{KamuApiServerClient, KamuApiServerClientExt};
use kamu_cli_puppet::extensions::{KamuCliPuppetExt, RepoAlias};
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::{
    AccountName,
    AddPushSource,
    DatasetAlias,
    DatasetName,
    DatasetSnapshot,
    MergeStrategyLedger,
    ReadStepNdJson,
    SetTransform,
    SetVocab,
    SourceState,
    Transform,
    TransformInput,
    TransformSql,
};
use reqwest::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_sequence(kamu_api_server_client: KamuApiServerClient) {
    let dataset_name = DatasetName::new_unchecked("player-scores");

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    let kamu_api_server_dataset_endpoint = {
        let base_url = kamu_api_server_client.get_base_url();

        let mut dataset_endpoint = Url::parse("odf+http://host").unwrap();

        dataset_endpoint.set_host(base_url.host_str()).unwrap();
        dataset_endpoint.set_port(base_url.port()).unwrap();

        dataset_endpoint
            .join("e2e-user/player-scores")
            .unwrap()
            .to_string()
    };

    // 2. Pushing the dataset to the API server
    {
        let kamu_in_push_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // 2.1. Add the dataset
        {
            kamu_in_push_workspace
                .add_dataset(DatasetSnapshot {
                    name: DatasetAlias::new(None, dataset_name.clone()),
                    kind: opendatafabric::DatasetKind::Root,
                    metadata: vec![
                        AddPushSource {
                            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
                            read: ReadStepNdJson {
                                schema: Some(vec![
                                    "match_time TIMESTAMP".to_owned(),
                                    "match_id BIGINT".to_owned(),
                                    "player_id STRING".to_owned(),
                                    "score BIGINT".to_owned(),
                                ]),
                                ..Default::default()
                            }
                            .into(),
                            preprocess: None,
                            merge: MergeStrategyLedger {
                                primary_key: vec!["match_id".to_owned(), "player_id".to_owned()],
                            }
                            .into(),
                        }
                        .into(),
                        SetVocab {
                            event_time_column: Some("match_time".to_owned()),
                            ..Default::default()
                        }
                        .into(),
                    ],
                })
                .await;
        }

        // 2.1. Ingest data to the dataset
        {
            let data = indoc::indoc!(
                r#"
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
                "#,
            );

            kamu_in_push_workspace
                .ingest_data(&dataset_name, data)
                .await;
        }

        // 2.2. Login to the API server
        kamu_in_push_workspace
            .execute([
                "login",
                kamu_api_server_client.get_base_url().as_str(),
                "--access-token",
                token.as_str(),
            ])
            .await
            .success();

        // 2.3. Push the dataset to the API server
        kamu_in_push_workspace
            .execute([
                "push",
                "player-scores",
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ])
            .await
            .success();
    }

    // 3. Pulling the dataset from the API server
    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        kamu_in_pull_workspace
            .execute(["pull", kamu_api_server_dataset_endpoint.as_str()])
            .await
            .success();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_force_push_pull(kamu_api_server_client: KamuApiServerClient) {
    let dataset_name = DatasetName::new_unchecked("player-scores");
    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    let kamu_api_server_dataset_endpoint = {
        let base_url = kamu_api_server_client.get_base_url();

        let mut dataset_endpoint = Url::parse("odf+http://host").unwrap();

        dataset_endpoint.set_host(base_url.host_str()).unwrap();
        dataset_endpoint.set_port(base_url.port()).unwrap();

        dataset_endpoint
            .join(format!("e2e-user/{dataset_name}").as_str())
            .unwrap()
            .to_string()
    };

    // 2. Pushing the dataset to the API server
    {
        let kamu_in_push_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // 2.1. Add the dataset
        {
            kamu_in_push_workspace
                .add_dataset(DatasetSnapshot {
                    name: DatasetAlias::new(None, dataset_name.clone()),
                    kind: opendatafabric::DatasetKind::Root,
                    metadata: vec![
                        AddPushSource {
                            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
                            read: ReadStepNdJson {
                                schema: Some(vec![
                                    "match_time TIMESTAMP".to_owned(),
                                    "match_id BIGINT".to_owned(),
                                    "player_id STRING".to_owned(),
                                    "score BIGINT".to_owned(),
                                ]),
                                ..Default::default()
                            }
                            .into(),
                            preprocess: None,
                            merge: MergeStrategyLedger {
                                primary_key: vec!["match_id".to_owned(), "player_id".to_owned()],
                            }
                            .into(),
                        }
                        .into(),
                        SetVocab {
                            event_time_column: Some("match_time".to_owned()),
                            ..Default::default()
                        }
                        .into(),
                    ],
                })
                .await;
        }

        // 2.1. Ingest data to the dataset
        {
            let data = indoc::indoc!(
                r#"
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
                "#,
            );

            kamu_in_push_workspace
                .ingest_data(&dataset_name, data)
                .await;
        }

        // 2.2. Login to the API server
        kamu_in_push_workspace
            .execute([
                "login",
                kamu_api_server_client.get_base_url().as_str(),
                "--access-token",
                token.as_str(),
            ])
            .await
            .success();

        // Initial dataset push
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ])
            .await
            .success();

        // Hard compact dataset
        kamu_in_push_workspace
            .execute([
                "--yes",
                "system",
                "compact",
                dataset_name.as_str(),
                "--hard",
                "--keep-metadata-only",
            ])
            .await
            .success();

        // Should fail without force flag
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ])
            .await
            .failure();

        // Should successfully push with force flag
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
                "--force",
            ])
            .await
            .success();
    }

    // 3. Pulling the dataset from the API server
    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // Call with no-alias flag to avoid remote ingest checking in next step
        kamu_in_pull_workspace
            .execute([
                "pull",
                kamu_api_server_dataset_endpoint.as_str(),
                "--no-alias",
            ])
            .await
            .success();

        // Ingest data in pulled dataset
        let data = indoc::indoc!(
            r#"
                {"match_time": "2000-01-01", "match_id": 1, "player_id": "Chuck", "score": 90}
            "#,
        );

        kamu_in_pull_workspace
            .ingest_data(&dataset_name, data)
            .await;

        // Should fail without force flag
        kamu_in_pull_workspace
            .execute(["pull", kamu_api_server_dataset_endpoint.as_str()])
            .await
            .failure();

        // Should successfully pull with force flag
        kamu_in_pull_workspace
            .execute(["pull", kamu_api_server_dataset_endpoint.as_str(), "--force"])
            .await
            .success();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_add_alias(kamu_api_server_client: KamuApiServerClient) {
    let dataset_name = DatasetName::new_unchecked("player-scores");
    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    let kamu_api_server_dataset_endpoint = {
        let base_url = kamu_api_server_client.get_base_url();

        let mut dataset_endpoint = Url::parse("odf+http://host").unwrap();

        dataset_endpoint.set_host(base_url.host_str()).unwrap();
        dataset_endpoint.set_port(base_url.port()).unwrap();

        dataset_endpoint
            .join(format!("e2e-user/{dataset_name}").as_str())
            .unwrap()
            .to_string()
    };

    // 2. Push command
    {
        let kamu_in_push_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // Add the dataset
        {
            kamu_in_push_workspace
                .add_dataset(DatasetSnapshot {
                    name: DatasetAlias::new(None, dataset_name.clone()),
                    kind: opendatafabric::DatasetKind::Root,
                    metadata: vec![
                        AddPushSource {
                            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
                            read: ReadStepNdJson {
                                schema: Some(vec![
                                    "match_time TIMESTAMP".to_owned(),
                                    "match_id BIGINT".to_owned(),
                                    "player_id STRING".to_owned(),
                                    "score BIGINT".to_owned(),
                                ]),
                                ..Default::default()
                            }
                            .into(),
                            preprocess: None,
                            merge: MergeStrategyLedger {
                                primary_key: vec!["match_id".to_owned(), "player_id".to_owned()],
                            }
                            .into(),
                        }
                        .into(),
                        SetVocab {
                            event_time_column: Some("match_time".to_owned()),
                            ..Default::default()
                        }
                        .into(),
                    ],
                })
                .await;
        }

        // Ingest data to the dataset
        {
            let data = indoc::indoc!(
                r#"
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
                "#,
            );

            kamu_in_push_workspace
                .ingest_data(&dataset_name, data)
                .await;
        }

        // Login to the API server
        kamu_in_push_workspace
            .execute([
                "login",
                kamu_api_server_client.get_base_url().as_str(),
                "--access-token",
                token.as_str(),
            ])
            .await
            .success();

        // Dataset push without storing alias
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
                "--no-alias",
            ])
            .await
            .success();

        // Check alias should be empty
        let aliases = kamu_in_push_workspace
            .get_list_of_repo_aliases(&opendatafabric::DatasetRef::from(dataset_name.clone()))
            .await;
        assert!(aliases.is_empty());

        // Dataset push with storing alias
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ])
            .await
            .success();

        let aliases = kamu_in_push_workspace
            .get_list_of_repo_aliases(&opendatafabric::DatasetRef::from(dataset_name.clone()))
            .await;
        assert_eq!(
            aliases,
            vec![RepoAlias {
                dataset: dataset_name.clone(),
                kind: "Push".to_string(),
                alias: kamu_api_server_dataset_endpoint.clone(),
            }]
        )
    }

    // 3. Pull command
    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // Dataset pull without storing alias
        kamu_in_pull_workspace
            .execute([
                "pull",
                kamu_api_server_dataset_endpoint.as_str(),
                "--no-alias",
            ])
            .await
            .success();

        // Check alias should be empty
        let aliases = kamu_in_pull_workspace
            .get_list_of_repo_aliases(&opendatafabric::DatasetRef::from(dataset_name.clone()))
            .await;
        assert!(aliases.is_empty());

        // Delete local dataset
        kamu_in_pull_workspace
            .execute(["--yes", "delete", dataset_name.as_str()])
            .await
            .success();

        // Dataset pull with storing alias
        kamu_in_pull_workspace
            .execute(["pull", kamu_api_server_dataset_endpoint.as_str()])
            .await
            .success();

        let aliases = kamu_in_pull_workspace
            .get_list_of_repo_aliases(&opendatafabric::DatasetRef::from(dataset_name.clone()))
            .await;
        assert_eq!(
            aliases,
            vec![RepoAlias {
                dataset: dataset_name.clone(),
                kind: "Pull".to_string(),
                alias: kamu_api_server_dataset_endpoint.clone(),
            }]
        )
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_as(kamu_api_server_client: KamuApiServerClient) {
    let dataset_name = DatasetName::new_unchecked("player-scores");
    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;
    kamu_api_server_client
        .create_player_scores_dataset_with_data(
            &token,
            Some(&AccountName::new_unchecked("e2e-user")),
        )
        .await;

    let kamu_api_server_dataset_endpoint = {
        let base_url = kamu_api_server_client.get_base_url();

        let mut dataset_endpoint = Url::parse("odf+http://host").unwrap();

        dataset_endpoint.set_host(base_url.host_str()).unwrap();
        dataset_endpoint.set_port(base_url.port()).unwrap();

        dataset_endpoint
            .join(format!("e2e-user/{dataset_name}").as_str())
            .unwrap()
            .to_string()
    };

    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        let new_dataset_name = DatasetName::new_unchecked("foo");
        kamu_in_pull_workspace
            .execute([
                "pull",
                kamu_api_server_dataset_endpoint.as_str(),
                "--as",
                new_dataset_name.as_str(),
            ])
            .await
            .success();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_all(kamu_api_server_client: KamuApiServerClient) {
    let dataset_name = DatasetName::new_unchecked("player-scores");

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    let kamu_api_server_dataset_endpoint = {
        let base_url = kamu_api_server_client.get_base_url();

        let mut dataset_endpoint = Url::parse("odf+http://host").unwrap();

        dataset_endpoint.set_host(base_url.host_str()).unwrap();
        dataset_endpoint.set_port(base_url.port()).unwrap();

        dataset_endpoint
            .join("e2e-user/player-scores")
            .unwrap()
            .to_string()
    };

    // 2. Pushing the dataset to the API server
    {
        let kamu_in_push_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // 2.1. Add datasets
        {
            kamu_in_push_workspace
                .add_dataset(DatasetSnapshot {
                    name: DatasetAlias::new(None, dataset_name.clone()),
                    kind: opendatafabric::DatasetKind::Root,
                    metadata: vec![
                        AddPushSource {
                            source_name: SourceState::DEFAULT_SOURCE_NAME.to_string(),
                            read: ReadStepNdJson {
                                schema: Some(vec![
                                    "match_time TIMESTAMP".to_owned(),
                                    "match_id BIGINT".to_owned(),
                                    "player_id STRING".to_owned(),
                                    "score BIGINT".to_owned(),
                                ]),
                                ..Default::default()
                            }
                            .into(),
                            preprocess: None,
                            merge: MergeStrategyLedger {
                                primary_key: vec!["match_id".to_owned(), "player_id".to_owned()],
                            }
                            .into(),
                        }
                        .into(),
                        SetVocab {
                            event_time_column: Some("match_time".to_owned()),
                            ..Default::default()
                        }
                        .into(),
                    ],
                })
                .await;

            kamu_in_push_workspace
                .add_dataset(DatasetSnapshot {
                    name: DatasetAlias::new(None, dataset_name.clone()),
                    kind: opendatafabric::DatasetKind::Derivative,
                    metadata: vec![MetadataFactory::set_transform()
                        .inputs_from_refs([dataset_name.clone()])
                        .transform(
                            MetadataFactory::transform()
                                .engine("datafusion")
                                .query(
                                    "SELECT
                                        match_time,
                                        match_id,
                                        player_id,
                                        score
                                    FROM player-scores",
                                )
                                .build(),
                        )
                        .build()
                        .into()],
                })
                .await;
        }

        // 2.1. Ingest data to the dataset
        {
            let data = indoc::indoc!(
                r#"
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
                "#,
            );

            kamu_in_push_workspace
                .ingest_data(&dataset_name, data)
                .await;
        }

        // 2.2. Login to the API server
        kamu_in_push_workspace
            .execute([
                "login",
                kamu_api_server_client.get_base_url().as_str(),
                "--access-token",
                token.as_str(),
            ])
            .await
            .success();

        // Push all datasets should fail
        kamu_in_push_workspace
            .execute(["push", "--all"])
            .await
            .failure();
    }

    // 3. Pulling the dataset from the API server
    // {
    //     let kamu_in_pull_workspace =
    // KamuCliPuppet::new_workspace_tmp().await;

    //     kamu_in_pull_workspace
    //         .execute(["pull", kamu_api_server_dataset_endpoint.as_str()])
    //         .await
    //         .success();
    // }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
