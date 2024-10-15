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
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR,
    E2E_USER_ACCOUNT_NAME_STR,
};
use kamu_cli_puppet::extensions::{KamuCliPuppetExt, RepoAlias};
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::{AccountName, DatasetAlias, DatasetName};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_sequence(kamu_api_server_client: KamuApiServerClient) {
    let dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("player-scores"),
    );
    let kamu_api_server_dataset_endpoint =
        kamu_api_server_client.get_dataset_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

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
            kamu_in_push_workspace
                .ingest_data(
                    &dataset_alias.dataset_name,
                    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
                )
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
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) pushed",
        )
        .await;
    }

    // 3. Pulling the dataset from the API server
    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", kamu_api_server_dataset_endpoint.as_str()],
            "1 dataset(s) updated",
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_force_push_pull(kamu_api_server_client: KamuApiServerClient) {
    let dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("player-scores"),
    );
    let kamu_api_server_dataset_endpoint =
        kamu_api_server_client.get_dataset_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

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
            kamu_in_push_workspace
                .ingest_data(
                    &dataset_alias.dataset_name,
                    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
                )
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
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) pushed",
        )
        .await;

        // Hard compact dataset
        kamu_in_push_workspace
            .execute([
                "--yes",
                "system",
                "compact",
                dataset_alias.dataset_name.as_str(),
                "--hard",
                "--keep-metadata-only",
            ])
            .await
            .success();

        // Should fail without force flag
        run_and_assert_command_failure(
            &kamu_in_push_workspace,
            vec![
                "push",
                dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ],
            "Failed to push 1 dataset(s)",
        )
        .await;

        // Should successfully push with force flag
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
                "--force",
            ],
            "1 dataset(s) pushed",
        )
        .await;
    }

    // 3. Pulling the dataset from the API server
    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // Call with no-alias flag to avoid remote ingest checking in next step
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec![
                "pull",
                kamu_api_server_dataset_endpoint.as_str(),
                "--no-alias",
            ],
            "1 dataset(s) updated",
        )
        .await;

        // Ingest data in pulled dataset

        kamu_in_pull_workspace
            .ingest_data(
                &dataset_alias.dataset_name,
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
            )
            .await;

        // Should fail without force flag
        run_and_assert_command_failure(
            &kamu_in_pull_workspace,
            vec!["pull", kamu_api_server_dataset_endpoint.as_str()],
            "Failed to update 1 dataset(s)",
        )
        .await;

        // Should successfully pull with force flag
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", kamu_api_server_dataset_endpoint.as_str(), "--force"],
            "1 dataset(s) updated",
        )
        .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_add_alias(kamu_api_server_client: KamuApiServerClient) {
    let dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("player-scores"),
    );
    let kamu_api_server_dataset_endpoint =
        kamu_api_server_client.get_dataset_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

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
            kamu_in_push_workspace
                .ingest_data(
                    &dataset_alias.dataset_name,
                    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
                )
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
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
                "--no-alias",
            ],
            "1 dataset(s) pushed",
        )
        .await;

        // Check alias should be empty
        let aliases = kamu_in_push_workspace
            .get_list_of_repo_aliases(&dataset_alias.dataset_name.clone().into())
            .await;
        assert!(aliases.is_empty());

        // Dataset push with storing alias
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) up-to-date",
        )
        .await;

        let aliases = kamu_in_push_workspace
            .get_list_of_repo_aliases(&dataset_alias.dataset_name.clone().into())
            .await;
        let expected_aliases = vec![RepoAlias {
            dataset: dataset_alias.dataset_name.clone(),
            kind: "Push".to_string(),
            alias: kamu_api_server_dataset_endpoint.to_string(),
        }];
        pretty_assertions::assert_eq!(aliases, expected_aliases);
    }

    // 3. Pull command
    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // Dataset pull without storing alias
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec![
                "pull",
                kamu_api_server_dataset_endpoint.as_str(),
                "--no-alias",
            ],
            "1 dataset(s) updated",
        )
        .await;

        // Check alias should be empty
        let aliases = kamu_in_pull_workspace
            .get_list_of_repo_aliases(&dataset_alias.dataset_name.clone().into())
            .await;
        assert!(aliases.is_empty());

        // Delete local dataset
        kamu_in_pull_workspace
            .execute(["--yes", "delete", dataset_alias.dataset_name.as_str()])
            .await
            .success();

        // Dataset pull with storing alias
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", kamu_api_server_dataset_endpoint.as_str()],
            "1 dataset(s) updated",
        )
        .await;

        let aliases = kamu_in_pull_workspace
            .get_list_of_repo_aliases(&dataset_alias.dataset_name.clone().into())
            .await;
        let expected_aliases = vec![RepoAlias {
            dataset: dataset_alias.dataset_name.clone(),
            kind: "Pull".to_string(),
            alias: kamu_api_server_dataset_endpoint.to_string(),
        }];
        pretty_assertions::assert_eq!(aliases, expected_aliases);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_as(kamu_api_server_client: KamuApiServerClient) {
    let dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("player-scores"),
    );
    let kamu_api_server_dataset_endpoint =
        kamu_api_server_client.get_dataset_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    kamu_api_server_client
        .create_player_scores_dataset_with_data(
            &token,
            Some(AccountName::new_unchecked(E2E_USER_ACCOUNT_NAME_STR)),
        )
        .await;

    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        let new_dataset_name = DatasetName::new_unchecked("foo");
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec![
                "pull",
                kamu_api_server_dataset_endpoint.as_str(),
                "--as",
                new_dataset_name.as_str(),
            ],
            "1 dataset(s) updated",
        )
        .await;

        let expected_dataset_list = kamu_in_pull_workspace
            .list_datasets()
            .await
            .into_iter()
            .map(|dataset| dataset.name)
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(vec![new_dataset_name], expected_dataset_list);
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_all(kamu_api_server_client: KamuApiServerClient) {
    let root_dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("player-scores"),
    );
    let kamu_api_server_root_dataset_endpoint =
        kamu_api_server_client.get_dataset_endpoint(&root_dataset_alias);

    let derivative_dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("leaderboard"),
    );
    let kamu_api_server_derivative_dataset_endpoint =
        kamu_api_server_client.get_dataset_endpoint(&derivative_dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    let mut kamu_in_push_workspace = KamuCliPuppet::new_workspace_tmp().await;

    // 2. Pushing datasets to the API server
    {
        kamu_in_push_workspace
            .set_system_time(Some(DateTime::from_str("2050-01-02T03:04:05Z").unwrap()));

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
            kamu_in_push_workspace
                .ingest_data(
                    &root_dataset_alias.dataset_name,
                    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
                )
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
        run_and_assert_command_failure(
            &kamu_in_push_workspace,
            vec!["push", "--all"],
            "Pushing all datasets is not yet supported",
        )
        .await;

        // Push datasets one by one
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                root_dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_root_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) pushed",
        )
        .await;

        kamu_in_push_workspace
            .execute(["pull", derivative_dataset_alias.dataset_name.as_str()])
            .await
            .success();

        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                derivative_dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_derivative_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) pushed",
        )
        .await;
    }

    // 3. Pulling datasets from the API server
    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        // Pull datasets one by one and check data
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", kamu_api_server_root_dataset_endpoint.as_str()],
            "1 dataset(s) updated",
        )
        .await;
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", kamu_api_server_derivative_dataset_endpoint.as_str()],
            "1 dataset(s) updated",
        )
        .await;

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
            | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1        | Alice     | 100   |
            | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1        | Bob       | 80    |
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
            | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
            | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(
                &root_dataset_alias.dataset_name,
                expected_schema,
                expected_data,
            )
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &derivative_dataset_alias.dataset_name,
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;

        // Update remote datasets

        kamu_in_push_workspace
            .ingest_data(
                &root_dataset_alias.dataset_name,
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
            )
            .await;
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec!["pull", derivative_dataset_alias.dataset_name.as_str()],
            "1 dataset(s) updated",
        )
        .await;

        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                root_dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_root_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) pushed",
        )
        .await;
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                derivative_dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_derivative_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) pushed",
        )
        .await;

        // Pull all datasets
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", "--all"],
            "2 dataset(s) updated",
        )
        .await;

        // Perform dataslices checks
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 2      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2        | Charlie   | 90    |
            | 3      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2        | Alice     | 70    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        let expected_derivative_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 2      | 1  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            | 3      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2     | 2        | Charlie   | 90    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(
                &root_dataset_alias.dataset_name,
                expected_schema,
                expected_data,
            )
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &derivative_dataset_alias.dataset_name,
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_recursive(kamu_api_server_client: KamuApiServerClient) {
    let root_dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("player-scores"),
    );
    let kamu_api_server_root_dataset_endpoint =
        kamu_api_server_client.get_dataset_endpoint(&root_dataset_alias);

    let derivative_dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("leaderboard"),
    );

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

    let mut kamu_in_push_workspace = KamuCliPuppet::new_workspace_tmp().await;

    // 2. Pushing datasets to the API server
    {
        kamu_in_push_workspace
            .set_system_time(Some(DateTime::from_str("2050-01-02T03:04:05Z").unwrap()));

        // 2.1. Add datasets
        {
            kamu_in_push_workspace
                .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
                .await
                .success();
        }

        // 2.1. Ingest data to the dataset
        {
            kamu_in_push_workspace
                .ingest_data(
                    &root_dataset_alias.dataset_name,
                    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
                )
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
        run_and_assert_command_failure(
            &kamu_in_push_workspace,
            vec![
                "push",
                root_dataset_alias.dataset_name.as_str(),
                "--recursive",
            ],
            "Recursive push is not yet supported",
        )
        .await;

        // Push dataset
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                root_dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_root_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) pushed",
        )
        .await;
    }

    // 3. Pulling datasets from the API server
    {
        let mut kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;
        kamu_in_pull_workspace
            .set_system_time(Some(DateTime::from_str("2050-01-02T03:04:05Z").unwrap()));

        // Pull datasets one by one and check data
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", kamu_api_server_root_dataset_endpoint.as_str()],
            "1 dataset(s) updated",
        )
        .await;

        kamu_in_pull_workspace
            .execute_with_input(
                ["add", "--stdin"],
                DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
            )
            .await
            .success();

        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", derivative_dataset_alias.dataset_name.as_str()],
            "1 dataset(s) updated",
        )
        .await;

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
            | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1        | Alice     | 100   |
            | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1        | Bob       | 80    |
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
            | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
            | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(
                &root_dataset_alias.dataset_name,
                expected_schema,
                expected_data,
            )
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &derivative_dataset_alias.dataset_name,
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;

        // Update remote datasets

        kamu_in_push_workspace
            .ingest_data(
                &root_dataset_alias.dataset_name,
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
            )
            .await;
        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                root_dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_root_dataset_endpoint.as_str(),
            ],
            "1 dataset(s) pushed",
        )
        .await;

        // Pull all datasets
        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec![
                "pull",
                derivative_dataset_alias.dataset_name.as_str(),
                "--recursive",
            ],
            "2 dataset(s) updated",
        )
        .await;

        // Perform dataslices checks
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 2      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2        | Charlie   | 90    |
            | 3      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2        | Alice     | 70    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        let expected_derivative_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 2      | 1  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
            | 3      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2     | 2        | Charlie   | 90    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(
                &root_dataset_alias.dataset_name,
                expected_schema,
                expected_data,
            )
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &derivative_dataset_alias.dataset_name,
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
            "2051-01-02T03:04:05Z",
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

pub async fn test_smart_pull_reset_derivative(kamu: KamuCliPuppet) {
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

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    run_and_assert_command_success(
        &kamu,
        vec!["pull", dataset_derivative_name.as_str()],
        "1 dataset(s) updated",
    )
    .await;

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
        | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1     | 1        | Alice     | 100   |
        | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 2     | 1        | Bob       | 80    |
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
    run_and_assert_command_failure(
        &kamu,
        vec!["pull", dataset_derivative_name.as_str()],
        "Failed to update 1 dataset(s)",
    )
    .await;

    // Add new data to root dataset
    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2,
    )
    .await;

    run_and_assert_command_success(
        &kamu,
        vec![
            "pull",
            dataset_derivative_name.as_str(),
            "--reset-derivatives-on-diverged-input",
        ],
        "1 dataset(s) updated",
    )
    .await;

    let expected_derivative_data = indoc::indoc!(
        r#"
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | offset | op | system_time          | match_time           | place | match_id | player_id | score |
        +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
        | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2     | 2        | Alice     | 70    |
        | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 1     | 2        | Charlie   | 90    |
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
    let dataset_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("player-scores"),
    );
    let kamu_api_server_dataset_endpoint =
        kamu_api_server_client.get_dataset_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.login_as_e2e_user().await;

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
            kamu_in_push_workspace
                .ingest_data(
                    &dataset_alias.dataset_name,
                    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
                )
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

        run_and_assert_command_success(
            &kamu_in_push_workspace,
            vec![
                "push",
                dataset_alias.dataset_name.as_str(),
                "--to",
                kamu_api_server_dataset_endpoint.as_str(),
                "--visibility",
                "private",
            ],
            "1 dataset(s) pushed",
        )
        .await;

        // ToDo add visibility check
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_pull_s3(kamu: KamuCliPuppet) {
    let dataset_name = DatasetName::new_unchecked("player-scores");

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    let s3_server = LocalS3Server::new().await;

    let dataset_url = format!("{}/e2e-user/{dataset_name}", s3_server.url);
    // Push dataset
    run_and_assert_command_success(
        &kamu,
        vec!["push", dataset_name.as_str(), "--to", dataset_url.as_str()],
        "1 dataset(s) pushed",
    )
    .await;

    {
        let kamu_in_pull_workspace = KamuCliPuppet::new_workspace_tmp().await;

        run_and_assert_command_success(
            &kamu_in_pull_workspace,
            vec!["pull", dataset_url.as_str()],
            "1 dataset(s) updated",
        )
        .await;

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
            | 0      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1        | Alice     | 100   |
            | 1      | 0  | 2050-01-02T03:04:05Z | 2000-01-01T00:00:00Z | 1        | Bob       | 80    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        kamu.assert_last_data_slice(&dataset_name, expected_schema, expected_data)
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_derivative(kamu: KamuCliPuppet) {
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

    kamu.ingest_data(
        &dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    run_and_assert_command_failure(
        &kamu,
        vec![
            "tail",
            dataset_derivative_name.as_str(),
            "--output-format",
            "table",
        ],
        "Error: Dataset schema is not yet available: leaderboard",
    )
    .await;

    run_and_assert_command_success(
        &kamu,
        vec!["pull", dataset_derivative_name.as_str()],
        "1 dataset(s) updated",
    )
    .await;

    {
        let assert = kamu
            .execute([
                "sql",
                "--engine",
                "datafusion",
                "--command",
                // Without unstable "offset" column.
                // For a beautiful output, cut to seconds
                indoc::indoc!(
                    r#"
                    SELECT op,
                           system_time,
                           DATE_TRUNC('second', match_time) as match_time,
                           match_id,
                           player_id,
                           score
                    FROM "player-scores"
                    ORDER BY match_time;
                    "#
                ),
                "--output-format",
                "table",
            ])
            .await
            .success();

        let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

        pretty_assertions::assert_eq!(
            indoc::indoc!(
                r#"
                ┌────┬──────────────────────┬──────────────────────┬──────────┬───────────┬───────┐
                │ op │     system_time      │      match_time      │ match_id │ player_id │ score │
                ├────┼──────────────────────┼──────────────────────┼──────────┼───────────┼───────┤
                │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │     Alice │   100 │
                │  0 │ 2050-01-02T03:04:05Z │ 2000-01-01T00:00:00Z │        1 │       Bob │    80 │
                └────┴──────────────────────┴──────────────────────┴──────────┴───────────┴───────┘
                "#
            ),
            stdout
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn run_and_assert_command_success(
    kamu: &KamuCliPuppet,
    args: Vec<&str>,
    expected_message: &str,
) {
    let assert = kamu.execute(args).await.success();

    let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(
        stderr.contains(expected_message),
        "Unexpected output:\n{stderr}",
    );
}

async fn run_and_assert_command_failure(
    kamu: &KamuCliPuppet,
    args: Vec<&str>,
    expected_message: &str,
) {
    let assert = kamu.execute(args).await.failure();

    let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(
        stderr.contains(expected_message),
        "Unexpected output:\n{stderr}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
