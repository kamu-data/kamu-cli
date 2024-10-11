// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use chrono::DateTime;
use kamu::testing::LocalS3Server;
use kamu_cli_e2e_common::{
    KamuApiServerClient,
    KamuApiServerClientExt,
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
};
use kamu_cli_puppet::extensions::{KamuCliPuppetExt, RepoAlias};
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::{AccountName, DatasetName};
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
                .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
                .await
                .success();
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
                .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
                .await
                .success();
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
                .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
                .await
                .success();
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
        );
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
        );
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
    let dataset_derivative_name = DatasetName::new_unchecked("leaderboard");

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    let (kamu_api_server_root_dataset_endpoint, kamu_api_server_derivative_dataset_endpoint) = {
        let base_url = kamu_api_server_client.get_base_url();

        let mut dataset_endpoint = Url::parse("odf+http://host").unwrap();

        dataset_endpoint.set_host(base_url.host_str()).unwrap();
        dataset_endpoint.set_port(base_url.port()).unwrap();

        (
            dataset_endpoint
                .clone()
                .join(format!("e2e-user/{dataset_name}").as_str())
                .unwrap()
                .to_string(),
            dataset_endpoint
                .clone()
                .join(format!("e2e-user/{dataset_derivative_name}").as_str())
                .unwrap()
                .to_string(),
        )
    };

    let mut kamu_in_push_workspace = KamuCliPuppet::new_workspace_tmp().await;

    // 2. Pushing datasets to the API server
    {
        kamu_in_push_workspace
            .set_system_time(Some(DateTime::from_str("2000-01-01T00:00:00Z").unwrap()));

        // 2.1. Add datasets
        {
            kamu_in_push_workspace
                .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
                .await
                .success();

            kamu_in_push_workspace
                .execute_with_input(
                    ["add", "--stdin"],
                    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
                )
                .await
                .success();
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

        // Push datasets one by one
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_root_dataset_endpoint.as_str(),
            ])
            .await
            .success();

        kamu_in_push_workspace
            .execute(["pull", dataset_derivative_name.as_str()])
            .await
            .success();

        kamu_in_push_workspace
            .execute([
                "push",
                dataset_derivative_name.as_str(),
                "--to",
                kamu_api_server_derivative_dataset_endpoint.as_str(),
            ])
            .await
            .success();
    }

    // 3. Pulling datasets from the API server
    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // Pull datasets one by one and check data
        kamu_in_pull_workspace
            .execute(["pull", kamu_api_server_root_dataset_endpoint.as_str()])
            .await
            .success();

        kamu_in_pull_workspace
            .execute(["pull", kamu_api_server_derivative_dataset_endpoint.as_str()])
            .await
            .success();

        let expected_schema = indoc::indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_id;
              OPTIONAL BYTE_ARRAY player_id (STRING);
              OPTIONAL INT64 score;
            }
            "#
        );
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 0      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1        | Alice     | 100   |
            | 1      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        let expected_derivative_schema = indoc::indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 place;
              OPTIONAL INT64 match_id;
              OPTIONAL BYTE_ARRAY player_id (STRING);
              OPTIONAL INT64 score;
            }
            "#
        );
        let expected_derivative_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 0      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
            | 1      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(&dataset_name, expected_schema, expected_data)
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &dataset_derivative_name,
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;

        // Update remote datasets
        let data = indoc::indoc!(
            r#"
                {"match_time": "2000-01-01", "match_id": 2, "player_id": "Bob", "score": 90}
            "#,
        );

        kamu_in_push_workspace
            .ingest_data(&dataset_name, data)
            .await;
        kamu_in_push_workspace
            .execute(["pull", dataset_derivative_name.as_str()])
            .await
            .success();
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_root_dataset_endpoint.as_str(),
            ])
            .await
            .success();
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_derivative_name.as_str(),
                "--to",
                kamu_api_server_derivative_dataset_endpoint.as_str(),
            ])
            .await
            .success();

        // Pull all datasets
        kamu_in_pull_workspace
            .execute(["pull", "--all"])
            .await
            .success();

        // Perform dataslices checks
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 2      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2        | Bob       | 90    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        let expected_derivative_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 2      | 1  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            | 3      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2     | 2        | Bob       | 90    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(&dataset_name, expected_schema, expected_data)
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &dataset_derivative_name,
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_recursive(kamu_api_server_client: KamuApiServerClient) {
    let dataset_name = DatasetName::new_unchecked("player-scores");
    let dataset_derivative_name = DatasetName::new_unchecked("leaderboard");

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    let kamu_api_server_root_dataset_endpoint = {
        let base_url = kamu_api_server_client.get_base_url();

        let mut dataset_endpoint = Url::parse("odf+http://host").unwrap();

        dataset_endpoint.set_host(base_url.host_str()).unwrap();
        dataset_endpoint.set_port(base_url.port()).unwrap();

        dataset_endpoint
            .clone()
            .join(format!("e2e-user/{dataset_name}").as_str())
            .unwrap()
            .to_string()
    };

    let mut kamu_in_push_workspace = KamuCliPuppet::new_workspace_tmp().await;

    // 2. Pushing datasets to the API server
    {
        kamu_in_push_workspace
            .set_system_time(Some(DateTime::from_str("2000-01-01T00:00:00Z").unwrap()));

        // 2.1. Add datasets
        {
            kamu_in_push_workspace
                .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
                .await
                .success();
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
            .execute(["push", dataset_name.as_str(), "--recursive"])
            .await
            .failure();

        // Push dataset
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_root_dataset_endpoint.as_str(),
            ])
            .await
            .success();
    }

    // 3. Pulling datasets from the API server
    {
        let mut kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;
        kamu_in_pull_workspace
            .set_system_time(Some(DateTime::from_str("2000-01-01T00:00:00Z").unwrap()));

        // Pull datasets one by one and check data
        kamu_in_pull_workspace
            .execute(["pull", kamu_api_server_root_dataset_endpoint.as_str()])
            .await
            .success();

        kamu_in_pull_workspace
            .execute_with_input(
                ["add", "--stdin"],
                DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
            )
            .await
            .success();

        kamu_in_pull_workspace
            .execute(["pull", dataset_derivative_name.as_str()])
            .await
            .success();

        let expected_schema = indoc::indoc!(
            r#"
            message arrow_schema {
              REQUIRED INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_id;
              OPTIONAL BYTE_ARRAY player_id (STRING);
              OPTIONAL INT64 score;
            }
            "#
        );
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 0      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1        | Alice     | 100   |
            | 1      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        let expected_derivative_schema = indoc::indoc!(
            r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 place;
              OPTIONAL INT64 match_id;
              OPTIONAL BYTE_ARRAY player_id (STRING);
              OPTIONAL INT64 score;
            }
            "#
        );
        let expected_derivative_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 0      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
            | 1      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(&dataset_name, expected_schema, expected_data)
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &dataset_derivative_name,
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;

        // Update remote datasets
        let data = indoc::indoc!(
            r#"
                {"match_time": "2000-01-01", "match_id": 2, "player_id": "Bob", "score": 90}
            "#,
        );

        kamu_in_push_workspace
            .ingest_data(&dataset_name, data)
            .await;
        kamu_in_push_workspace
            .execute([
                "push",
                dataset_name.as_str(),
                "--to",
                kamu_api_server_root_dataset_endpoint.as_str(),
            ])
            .await
            .success();

        // Pull all datasets
        kamu_in_pull_workspace
            .execute(["pull", dataset_derivative_name.as_str(), "--recursive"])
            .await
            .success();

        // Perform dataslices checks
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 2      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2        | Bob       | 90    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        let expected_derivative_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 2      | 1  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            | 3      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2     | 2        | Bob       | 90    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(&dataset_name, expected_schema, expected_data)
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &dataset_derivative_name,
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_set_watermark(kamu: KamuCliPuppet) {
    let dataset_name = DatasetName::new_unchecked("player-scores");

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    let assert = kamu
        .execute([
            "pull",
            dataset_name.as_str(),
            "--set-watermark",
            "2000-01-01T00:00:00Z",
        ])
        .await
        .success();

    let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(
        stderr.contains(indoc::indoc!(r#"Committed new block"#).trim()),
        "Unexpected output:\n{stderr}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_reset_derivative(mut kamu: KamuCliPuppet) {
    kamu.set_system_time(Some(DateTime::from_str("2000-01-01T00:00:00Z").unwrap()));
    let dataset_name = DatasetName::new_unchecked("player-scores");
    let dataset_derivative_name = DatasetName::new_unchecked("leaderboard");

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.execute_with_input(
        ["add", "--stdin"],
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    )
    .await
    .success();

    let data = indoc::indoc!(
        r#"
            {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
            {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
        "#,
    );

    kamu.ingest_data(&dataset_name, data).await;

    kamu.execute(["pull", dataset_derivative_name.as_str()])
        .await
        .success();

    let expected_derivative_schema = indoc::indoc!(
        r#"
            message arrow_schema {
              OPTIONAL INT64 offset;
              REQUIRED INT32 op;
              REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 match_time (TIMESTAMP(MILLIS,true));
              OPTIONAL INT64 place;
              OPTIONAL INT64 match_id;
              OPTIONAL BYTE_ARRAY player_id (STRING);
              OPTIONAL INT64 score;
            }
            "#
    );
    let expected_derivative_data = indoc::indoc!(
        r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 0      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
            | 1      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
    );
    kamu.assert_last_data_slice(
        &dataset_derivative_name,
        expected_derivative_schema,
        expected_derivative_data,
    )
    .await;

    // Compact root dataset
    kamu.execute([
        "--yes",
        "system",
        "compact",
        dataset_name.as_str(),
        "--hard",
        "--keep-metadata-only",
    ])
    .await
    .success();

    // Pull derivative should fail
    kamu.execute(["pull", dataset_derivative_name.as_str()])
        .await
        .failure();

    // Add new data to root dataset
    let data = indoc::indoc!(
        r#"
            {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
        "#,
    );

    kamu.ingest_data(&dataset_name, data).await;

    kamu.execute([
        "pull",
        dataset_derivative_name.as_str(),
        "--reset-derivatives-on-diverged-input",
    ])
    .await
    .success();

    let expected_derivative_data = indoc::indoc!(
        r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 0      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
    );
    kamu.assert_last_data_slice(
        &dataset_derivative_name,
        expected_derivative_schema,
        expected_derivative_data,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_visibility(kamu_api_server_client: KamuApiServerClient) {
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
                .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
                .await
                .success();
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

        kamu_in_push_workspace
            .execute([
                "push",
                "player-scores",
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
                "--visibility",
                "private",
            ])
            .await
            .success();

        // ToDo add visibility check
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_s3(mut kamu: KamuCliPuppet) {
    kamu.set_system_time(Some(DateTime::from_str("2000-01-01T00:00:00Z").unwrap()));
    let dataset_name = DatasetName::new_unchecked("player-scores");

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    let data = indoc::indoc!(
        r#"
            {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
            {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
        "#,
    );

    kamu.ingest_data(&dataset_name, data).await;

    let s3_server = LocalS3Server::new().await;

    let dataset_url = format!("{}/e2e-user/{dataset_name}", s3_server.url);
    // Push dataset
    kamu.execute(["push", dataset_name.as_str(), "--to", dataset_url.as_str()])
        .await
        .success();

    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        kamu_in_pull_workspace
            .execute(["pull", dataset_url.as_str()])
            .await
            .success();

        let expected_schema = indoc::indoc!(
            r#"
                    message arrow_schema {
                      REQUIRED INT64 offset;
                      REQUIRED INT32 op;
                      REQUIRED INT64 system_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 match_time (TIMESTAMP(MILLIS,true));
                      OPTIONAL INT64 match_id;
                      OPTIONAL BYTE_ARRAY player_id (STRING);
                      OPTIONAL INT64 score;
                    }
                    "#
        );
        let expected_data = indoc::indoc!(
            r#"
                    +--------+----+----------------------+----------------------+----------+-----------+-------+
                    | offset | op | system_time          | match_time           | match_id | player_id | score |
                    +--------+----+----------------------+----------------------+----------+-----------+-------+
                    | 0      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1        | Alice     | 100   |
                    | 1      | 0  | 2000-01-01T00:00:00Z | 2000-01-01T00:00:00Z | 1        | Bob       | 80    |
                    +--------+----+----------------------+----------------------+----------+-----------+-------+
                    "#
        );
        kamu.assert_last_data_slice(&dataset_name, expected_schema, expected_data)
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
