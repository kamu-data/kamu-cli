// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::assert_matches::assert_matches;
use std::str::FromStr;

use chrono::DateTime;
use kamu_cli_e2e_common::*;
use kamu_cli_puppet::extensions::{KamuCliPuppetExt, RepoAlias};
use kamu_cli_puppet::KamuCliPuppet;
use serde_json::json;
use test_utils::LocalS3Server;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

macro_rules! test_smart_transfer_protocol_permutations {
    ($test_name: expr) => {
        paste::paste! {
            pub async fn [<$test_name "_st_st">](kamu_api_server_client: KamuApiServerClient) {
                $test_name(kamu_api_server_client, false, false).await;
            }

            pub async fn [<$test_name "_st_mt">](kamu_api_server_client: KamuApiServerClient) {
                $test_name(kamu_api_server_client, false, true).await;
            }

            pub async fn [<$test_name "_mt_st">](kamu_api_server_client: KamuApiServerClient) {
                $test_name(kamu_api_server_client, true, false).await;
            }

            pub async fn [<$test_name "_mt_mt">](kamu_api_server_client: KamuApiServerClient) {
                $test_name(kamu_api_server_client, true, true).await;
            }
        }
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_smart_transfer_protocol_permutations!(test_smart_push_smart_pull_sequence);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_smart_transfer_protocol_permutations!(test_smart_push_force_smart_pull_force);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_smart_transfer_protocol_permutations!(test_smart_push_no_alias_smart_pull_no_alias);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_as_st(kamu_api_server_client: KamuApiServerClient) {
    test_smart_pull_as(kamu_api_server_client, false).await;
}

pub async fn test_smart_pull_as_mt(kamu_api_server_client: KamuApiServerClient) {
    test_smart_pull_as(kamu_api_server_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_pull_visibility_public_st(kamu_api_server_client: KamuApiServerClient) {
    test_smart_pull_visibility_public(kamu_api_server_client, false).await;
}

pub async fn test_smart_pull_visibility_public_mt(kamu_api_server_client: KamuApiServerClient) {
    test_smart_pull_visibility_public(kamu_api_server_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_smart_transfer_protocol_permutations!(test_smart_push_all_smart_pull_all);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_smart_transfer_protocol_permutations!(test_smart_push_recursive_smart_pull_recursive);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_visibility_st(kamu_api_server_client: KamuApiServerClient) {
    test_smart_push_visibility(kamu_api_server_client, false).await;
}

pub async fn test_smart_push_visibility_mt(kamu_api_server_client: KamuApiServerClient) {
    test_smart_push_visibility(kamu_api_server_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_simple_push_to_s3_smart_pull_st_st(kamu: KamuCliPuppet) {
    test_simple_push_to_s3_smart_pull(kamu, false).await;
}

pub async fn test_simple_push_to_s3_smart_pull_st_mt(kamu: KamuCliPuppet) {
    test_simple_push_to_s3_smart_pull(kamu, true).await;
}

pub async fn test_simple_push_to_s3_smart_pull_mt_st(kamu: KamuCliPuppet) {
    test_simple_push_to_s3_smart_pull(kamu, false).await;
}

pub async fn test_simple_push_to_s3_smart_pull_mt_mt(kamu: KamuCliPuppet) {
    test_simple_push_to_s3_smart_pull(kamu, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

test_smart_transfer_protocol_permutations!(test_smart_push_pull_with_registered_repo_smart_pull);

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_smart_push_trigger_dependent_dataset_update_st(
    kamu_api_server_client: KamuApiServerClient,
) {
    test_smart_push_trigger_dependent_dataset_update(kamu_api_server_client, false).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_push_smart_pull_sequence(
    mut kamu_api_server_client: KamuApiServerClient,
    is_push_workspace_multi_tenant: bool,
    is_pull_workspace_multi_tenant: bool,
) {
    let dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;

    // 2. Pushing the dataset to the API server
    {
        let kamu_in_push_workspace =
            KamuCliPuppet::new_workspace_tmp(is_push_workspace_multi_tenant).await;

        // 2.1. Add the dataset
        kamu_in_push_workspace
            .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
            .await
            .success();

        // 2.1. Ingest data to the dataset
        kamu_in_push_workspace
            .ingest_data(
                &dataset_alias.dataset_name,
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
            )
            .await;

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
            .assert_success_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--visibility",
                    "public",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
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
        kamu_in_push_workspace
            .assert_failure_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"Failed to push 1 dataset\(s\)"#]),
            )
            .await;

        // Should successfully push with force flag
        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--force",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;
    }

    // 3. Pulling the dataset from the API server
    {
        let kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;

        // Call with no-alias flag to avoid remote ingest checking in next step
        kamu_in_pull_workspace
            .assert_success_command_execution(
                [
                    "pull",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--no-alias",
                ],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
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
        kamu_in_pull_workspace
            .assert_failure_command_execution(
                ["pull", kamu_api_server_dataset_endpoint.as_str()],
                None,
                Some([r#"Failed to update 1 dataset\(s\)"#]),
            )
            .await;

        // Should successfully pull with force flag
        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", kamu_api_server_dataset_endpoint.as_str(), "--force"],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [(
                    dataset_alias.dataset_name,
                    Some(odf::DatasetVisibility::Private)
                )],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_push_force_smart_pull_force(
    mut kamu_api_server_client: KamuApiServerClient,
    is_push_workspace_multi_tenant: bool,
    is_pull_workspace_multi_tenant: bool,
) {
    let dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;

    // 2. Pushing the dataset to the API server
    {
        let kamu_in_push_workspace =
            KamuCliPuppet::new_workspace_tmp(is_push_workspace_multi_tenant).await;

        // 2.1. Add the dataset
        kamu_in_push_workspace
            .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
            .await
            .success();

        // 2.1. Ingest data to the dataset
        kamu_in_push_workspace
            .ingest_data(
                &dataset_alias.dataset_name,
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
            )
            .await;

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
            .assert_success_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--visibility",
                    "public",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
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
        kamu_in_push_workspace
            .assert_failure_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"Failed to push 1 dataset\(s\)"#]),
            )
            .await;

        // Should successfully push with force flag
        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--force",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;
    }

    // 3. Pulling the dataset from the API server
    {
        let kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;

        // Call with no-alias flag to avoid remote ingest checking in next step
        kamu_in_pull_workspace
            .assert_success_command_execution(
                [
                    "pull",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--no-alias",
                ],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
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
        kamu_in_pull_workspace
            .assert_failure_command_execution(
                ["pull", kamu_api_server_dataset_endpoint.as_str()],
                None,
                Some([r#"Failed to update 1 dataset\(s\)"#]),
            )
            .await;

        // Should successfully pull with force flag
        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", kamu_api_server_dataset_endpoint.as_str(), "--force"],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [(
                    dataset_alias.dataset_name,
                    Some(odf::DatasetVisibility::Private)
                )],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_push_no_alias_smart_pull_no_alias(
    mut kamu_api_server_client: KamuApiServerClient,
    is_push_workspace_multi_tenant: bool,
    is_pull_workspace_multi_tenant: bool,
) {
    let dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;

    // 2. Push command
    {
        let kamu_in_push_workspace =
            KamuCliPuppet::new_workspace_tmp(is_push_workspace_multi_tenant).await;

        // Add the dataset
        kamu_in_push_workspace
            .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
            .await
            .success();

        // Ingest data to the dataset
        kamu_in_push_workspace
            .ingest_data(
                &dataset_alias.dataset_name,
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
            )
            .await;

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
            .assert_success_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--no-alias",
                    "--visibility",
                    "public",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        // Check alias should be empty
        let aliases = kamu_in_push_workspace
            .get_list_of_repo_aliases(&dataset_alias.dataset_name.clone().into())
            .await;
        assert!(aliases.is_empty());

        // Dataset push with storing alias
        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) up-to-date"#]),
            )
            .await;

        let actual_aliases = kamu_in_push_workspace
            .get_list_of_repo_aliases(&dataset_alias.dataset_name.clone().into())
            .await
            .into_iter()
            .map(|repo_alias: RepoAlias| {
                (
                    repo_alias.dataset.dataset_name,
                    repo_alias.kind,
                    repo_alias.alias,
                )
            })
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(
            vec![(
                dataset_alias.dataset_name.clone(),
                "Push".to_string(),
                kamu_api_server_dataset_endpoint.to_string()
            )],
            actual_aliases
        );
    }

    // 3. Pull command
    {
        let kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;

        // Dataset pull without storing alias
        kamu_in_pull_workspace
            .assert_success_command_execution(
                [
                    "pull",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--no-alias",
                ],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
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
        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", kamu_api_server_dataset_endpoint.as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        let actual_aliases = kamu_in_pull_workspace
            .get_list_of_repo_aliases(&dataset_alias.dataset_name.clone().into())
            .await
            .into_iter()
            .map(|repo_alias: RepoAlias| {
                (
                    repo_alias.dataset.dataset_name,
                    repo_alias.kind,
                    repo_alias.alias,
                )
            })
            .collect::<Vec<_>>();

        pretty_assertions::assert_eq!(
            vec![(
                dataset_alias.dataset_name.clone(),
                "Pull".to_string(),
                kamu_api_server_dataset_endpoint.to_string()
            )],
            actual_aliases
        );

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [(
                    dataset_alias.dataset_name,
                    Some(odf::DatasetVisibility::Private)
                )],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_pull_as(
    mut kamu_api_server_client: KamuApiServerClient,
    is_pull_workspace_multi_tenant: bool,
) {
    let dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&dataset_alias);

    // 1. Grub a token
    kamu_api_server_client.auth().login_as_e2e_user().await;

    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset_with_data()
        .await;

    {
        let kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;
        let new_dataset_name = odf::DatasetName::new_unchecked("foo");

        kamu_in_pull_workspace
            .assert_success_command_execution(
                [
                    "pull",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--as",
                    new_dataset_name.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [(new_dataset_name, Some(odf::DatasetVisibility::Private))],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        } else {
            pretty_assertions::assert_eq!(
                [new_dataset_name],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| d.name)
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_pull_visibility_public(
    mut kamu_api_server_client: KamuApiServerClient,
    is_pull_workspace_multi_tenant: bool,
) {
    let dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&dataset_alias);

    // 1. Grub a token
    kamu_api_server_client.auth().login_as_e2e_user().await;

    kamu_api_server_client
        .dataset()
        .create_player_scores_dataset_with_data()
        .await;

    {
        let kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;

        kamu_in_pull_workspace
            .assert_success_command_execution(
                [
                    "pull",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--visibility",
                    "public",
                ],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [(
                    dataset_alias.dataset_name,
                    Some(odf::DatasetVisibility::Public)
                )],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        } else {
            pretty_assertions::assert_eq!(
                [dataset_alias.dataset_name],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| d.name)
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_push_all_smart_pull_all(
    mut kamu_api_server_client: KamuApiServerClient,
    is_push_workspace_multi_tenant: bool,
    is_pull_workspace_multi_tenant: bool,
) {
    let root_dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_root_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&root_dataset_alias);

    let derivative_dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_DERIVATIVE_LEADERBOARD_NAME.clone(),
    );
    let kamu_api_server_derivative_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&derivative_dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;

    let mut kamu_in_push_workspace =
        KamuCliPuppet::new_workspace_tmp(is_push_workspace_multi_tenant).await;

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
        kamu_in_push_workspace
            .assert_failure_command_execution(
                ["push", "--all"],
                None,
                Some(["Pushing all datasets is not yet supported"]),
            )
            .await;

        // Push datasets one by one
        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    root_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_root_dataset_endpoint.as_str(),
                    "--visibility",
                    "public",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        kamu_in_push_workspace
            .execute(["pull", derivative_dataset_alias.dataset_name.as_str()])
            .await
            .success();

        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    derivative_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_derivative_dataset_endpoint.as_str(),
                    "--visibility",
                    "public",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;
    }

    // 3. Pulling datasets from the API server
    {
        let kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;

        // Pull datasets one by one and check data
        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", kamu_api_server_root_dataset_endpoint.as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", kamu_api_server_derivative_dataset_endpoint.as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
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
              OPTIONAL INT64 place (INTEGER(64,false));
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
                &odf::DatasetAlias::new(None, root_dataset_alias.dataset_name.clone()),
                expected_schema,
                expected_data,
            )
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &odf::DatasetAlias::new(None, derivative_dataset_alias.dataset_name.clone()),
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

        kamu_in_push_workspace
            .assert_success_command_execution(
                ["pull", derivative_dataset_alias.dataset_name.as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    root_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_root_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    derivative_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_derivative_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        // Pull all datasets
        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", "--all"],
                None,
                Some([r#"2 dataset\(s\) updated"#]),
            )
            .await;

        // Perform dataslices checks
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 2      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2        | Alice     | 70    |
            | 3      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2        | Charlie   | 90    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        let expected_derivative_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 2      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 1     | 2        | Charlie   | 90    |
            | 3      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2     | 2        | Alice     | 70    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(
                &odf::DatasetAlias::new(None, root_dataset_alias.dataset_name.clone()),
                expected_schema,
                expected_data,
            )
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &odf::DatasetAlias::new(None, derivative_dataset_alias.dataset_name.clone()),
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [
                    (
                        derivative_dataset_alias.dataset_name,
                        Some(odf::DatasetVisibility::Private)
                    ),
                    (
                        root_dataset_alias.dataset_name,
                        Some(odf::DatasetVisibility::Private)
                    )
                ],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_push_recursive_smart_pull_recursive(
    mut kamu_api_server_client: KamuApiServerClient,
    is_push_workspace_multi_tenant: bool,
    is_pull_workspace_multi_tenant: bool,
) {
    let root_dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_root_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&root_dataset_alias);

    let derivative_dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_DERIVATIVE_LEADERBOARD_NAME.clone(),
    );

    // 1. Grub a token
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;

    let mut kamu_in_push_workspace =
        KamuCliPuppet::new_workspace_tmp(is_push_workspace_multi_tenant).await;

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
        kamu_in_push_workspace
            .assert_failure_command_execution(
                [
                    "push",
                    root_dataset_alias.dataset_name.as_str(),
                    "--recursive",
                ],
                None,
                Some(["Recursive push is not yet supported"]),
            )
            .await;

        // Push dataset
        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    root_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_root_dataset_endpoint.as_str(),
                    "--visibility",
                    "public",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;
    }

    // 3. Pulling datasets from the API server
    {
        let mut kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;

        kamu_in_pull_workspace
            .set_system_time(Some(DateTime::from_str("2050-01-02T03:04:05Z").unwrap()));

        // Pull datasets one by one and check data
        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", kamu_api_server_root_dataset_endpoint.as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        kamu_in_pull_workspace
            .execute_with_input(
                ["add", "--stdin"],
                DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
            )
            .await
            .success();

        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", derivative_dataset_alias.dataset_name.as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
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
              OPTIONAL INT64 place (INTEGER(64,false));
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
                &odf::DatasetAlias::new(None, root_dataset_alias.dataset_name.clone()),
                expected_schema,
                expected_data,
            )
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &odf::DatasetAlias::new(None, derivative_dataset_alias.dataset_name.clone()),
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

        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    root_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_root_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        // Pull all datasets
        kamu_in_pull_workspace
            .assert_success_command_execution(
                [
                    "pull",
                    derivative_dataset_alias.dataset_name.as_str(),
                    "--recursive",
                ],
                None,
                Some([r#"2 dataset\(s\) updated"#]),
            )
            .await;

        // Perform dataslices checks
        let expected_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | match_id | player_id | score |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            | 2      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2        | Alice     | 70    |
            | 3      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2        | Charlie   | 90    |
            +--------+----+----------------------+----------------------+----------+-----------+-------+
            "#
        );
        let expected_derivative_data = indoc::indoc!(
            r#"
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | offset | op | system_time          | match_time           | place | match_id | player_id | score |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            | 2      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 1     | 2        | Charlie   | 90    |
            | 3      | 0  | 2050-01-02T03:04:05Z | 2000-01-02T00:00:00Z | 2     | 2        | Alice     | 70    |
            +--------+----+----------------------+----------------------+-------+----------+-----------+-------+
            "#
        );

        kamu_in_pull_workspace
            .assert_last_data_slice(
                &odf::DatasetAlias::new(None, root_dataset_alias.dataset_name.clone()),
                expected_schema,
                expected_data,
            )
            .await;
        kamu_in_pull_workspace
            .assert_last_data_slice(
                &odf::DatasetAlias::new(None, derivative_dataset_alias.dataset_name.clone()),
                expected_derivative_schema,
                expected_derivative_data,
            )
            .await;

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [
                    (
                        derivative_dataset_alias.dataset_name,
                        Some(odf::DatasetVisibility::Private)
                    ),
                    (
                        root_dataset_alias.dataset_name,
                        Some(odf::DatasetVisibility::Private)
                    )
                ],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_push_visibility(
    mut kamu_api_server_client: KamuApiServerClient,
    is_push_workspace_multi_tenant: bool,
) {
    let dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;

    // 2. Pushing the dataset to the API server
    let dataset_id = {
        let kamu_in_push_workspace =
            KamuCliPuppet::new_workspace_tmp(is_push_workspace_multi_tenant).await;

        // 2.1. Add the dataset
        kamu_in_push_workspace
            .execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
            .await
            .success();

        // 2.1. Ingest data to the dataset
        kamu_in_push_workspace
            .ingest_data(
                &dataset_alias.dataset_name,
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
            )
            .await;

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
            .assert_success_command_execution(
                [
                    "push",
                    dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_dataset_endpoint.as_str(),
                    "--visibility",
                    "private",
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        let mut datasets = kamu_in_push_workspace.list_datasets().await;
        pretty_assertions::assert_eq!(1, datasets.len());
        datasets.swap_remove(0).id
    };

    assert_matches!(
        kamu_api_server_client
            .dataset()
            .get_visibility(&dataset_id)
            .await,
        Ok(odf::DatasetVisibility::Private)
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_simple_push_to_s3_smart_pull(
    kamu: KamuCliPuppet,
    is_pull_workspace_multi_tenant: bool,
) {
    let dataset_alias = odf::DatasetAlias::new(None, DATASET_ROOT_PLAYER_NAME.clone());

    kamu.execute_with_input(["add", "--stdin"], DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR)
        .await
        .success();

    kamu.ingest_data(
        &dataset_alias.dataset_name,
        DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    )
    .await;

    let s3_server = LocalS3Server::new().await;
    let dataset_url = format!("{}/e2e-user/{}", s3_server.url, dataset_alias.dataset_name);

    // Push dataset
    kamu.assert_success_command_execution(
        [
            "push",
            dataset_alias.dataset_name.as_str(),
            "--to",
            dataset_url.as_str(),
        ],
        None,
        Some([r#"1 dataset\(s\) pushed"#]),
    )
    .await;

    {
        let kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;

        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", dataset_url.as_str()],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
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
        kamu.assert_last_data_slice(&dataset_alias, expected_schema, expected_data)
            .await;

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [(
                    dataset_alias.dataset_name,
                    Some(odf::DatasetVisibility::Private)
                ),],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_push_pull_with_registered_repo_smart_pull(
    mut kamu_api_server_client: KamuApiServerClient,
    is_push_workspace_multi_tenant: bool,
    is_pull_workspace_multi_tenant: bool,
) {
    let dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let kamu_api_server_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;

    // 2. Pushing the dataset to the API server
    {
        let kamu_in_push_workspace =
            KamuCliPuppet::new_workspace_tmp(is_push_workspace_multi_tenant).await;

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
        // It will register new repo
        kamu_in_push_workspace
            .execute([
                "login",
                kamu_api_server_client.get_base_url().as_str(),
                "--access-token",
                token.as_str(),
            ])
            .await
            .success();

        // 2.3. Push the dataset to the API server without to argument
        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    "--visibility",
                    "public",
                    dataset_alias.dataset_name.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;
    }

    // 3. Pulling the dataset from the API server
    {
        let kamu_in_pull_workspace =
            KamuCliPuppet::new_workspace_tmp(is_pull_workspace_multi_tenant).await;

        // 3.2. Login to the API server
        // It will register new repo
        kamu_in_pull_workspace
            .execute([
                "login",
                kamu_api_server_client.get_base_url().as_str(),
                "--access-token",
                token.as_str(),
            ])
            .await
            .success();

        // ToDo: fix this behavior in scope of separation of pull and update commands
        let dataset_ref = if is_pull_workspace_multi_tenant {
            kamu_api_server_dataset_endpoint.as_str()
        } else {
            &format!(
                "{}/{}",
                kamu_api_server_client.get_base_url().host_str().unwrap(),
                dataset_alias.dataset_name
            )
        };

        kamu_in_pull_workspace
            .assert_success_command_execution(
                ["pull", dataset_ref],
                None,
                Some([r#"1 dataset\(s\) updated"#]),
            )
            .await;

        if is_pull_workspace_multi_tenant {
            pretty_assertions::assert_eq!(
                [(
                    dataset_alias.dataset_name,
                    Some(odf::DatasetVisibility::Private)
                ),],
                *kamu_in_pull_workspace
                    .list_datasets()
                    .await
                    .into_iter()
                    .map(|d| (d.name, d.visibility))
                    .collect::<Vec<_>>()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_smart_push_trigger_dependent_dataset_update(
    mut kamu_api_server_client: KamuApiServerClient,
    is_push_workspace_multi_tenant: bool,
) {
    let root_dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );
    let derivative_dataset_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_DERIVATIVE_LEADERBOARD_NAME.clone(),
    );
    let kamu_api_server_root_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&root_dataset_alias);
    let kamu_api_server_derivative_dataset_endpoint = kamu_api_server_client
        .dataset()
        .get_odf_endpoint(&derivative_dataset_alias);

    // 1. Grub a token
    let token = kamu_api_server_client.auth().login_as_e2e_user().await;

    // 2. Pushing the dataset to the API server
    {
        let kamu_in_push_workspace =
            KamuCliPuppet::new_workspace_tmp(is_push_workspace_multi_tenant).await;

        // 2.1. Add datasets
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

        // 2.1. Login to the API server
        kamu_in_push_workspace
            .execute([
                "login",
                kamu_api_server_client.get_base_url().as_str(),
                "--access-token",
                token.as_str(),
            ])
            .await
            .success();

        //2.2 Push datasets one by one
        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    root_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_root_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    derivative_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_derivative_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        //2.3 Enable batching trigger for derivative dataset
        let datasets = kamu_in_push_workspace.list_datasets().await;
        let derivative_dataset_id = datasets
            .iter()
            .find(|dataset| dataset.name == derivative_dataset_alias.dataset_name)
            .unwrap()
            .id
            .clone();

        kamu_api_server_client
            .graphql_api_call_assert(
                indoc::indoc!(
                    r#"
                mutation {
                    datasets {
                        byId (datasetId: $datasetId) {
                            flows {
                                triggers {
                                    setTrigger (
                                        datasetFlowType: $datasetFlowType,
                                        paused: false,
                                        triggerInput: {
                                            batching: {
                                                maxBatchingInterval: {
                                                    every: 0, unit: MINUTES
                                                },
                                                minRecordsToAwait: 0
                                            }
                                        }
                                    ) {
                                        __typename,
                                        message
                                        ... on SetFlowTriggerSuccess {
                                            trigger {
                                                __typename
                                                batching {
                                                    __typename
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "#
                )
                .replace("$datasetFlowType", "\"EXECUTE_TRANSFORM\"")
                .replace("$datasetId", &format!("\"{derivative_dataset_id}\""))
                .as_str(),
                Ok(indoc::indoc!(
                    r#"
                {
                  "datasets": {
                    "byId": {
                      "flows": {
                        "triggers": {
                          "setTrigger": {
                            "__typename": "SetFlowTriggerSuccess",
                            "message": "Success",
                            "trigger": {
                              "__typename": "FlowTrigger",
                              "batching": {
                                "__typename": "FlowTriggerBatchingRule"
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
                "#
                )),
            )
            .await;

        kamu_api_server_client
            .flow()
            .wait(&derivative_dataset_id, 1)
            .await;

        // Ingest data to the root dataset
        kamu_in_push_workspace
            .ingest_data(
                &root_dataset_alias.dataset_name,
                DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
            )
            .await;

        // Push root dataset
        kamu_in_push_workspace
            .assert_success_command_execution(
                [
                    "push",
                    root_dataset_alias.dataset_name.as_str(),
                    "--to",
                    kamu_api_server_root_dataset_endpoint.as_str(),
                ],
                None,
                Some([r#"1 dataset\(s\) pushed"#]),
            )
            .await;

        // Check derivative dataset data was updated
        kamu_api_server_client
            .flow()
            .wait(&derivative_dataset_id, 2)
            .await;

        let mut tail_result = kamu_api_server_client
            .odf_query()
            .tail(&derivative_dataset_alias)
            .await;

        if let Some(data) = tail_result.get_mut("data") {
            for entry in data.as_array_mut().unwrap() {
                entry.as_object_mut().unwrap().remove("system_time");
            }
        }

        pretty_assertions::assert_eq!(
            json!({
                "data": [
                    {
                        "match_id": 1,
                        "match_time": "2000-01-01T00:00:00Z",
                        "offset": 0,
                        "op": 0,
                        "player_id": "Alice",
                        "score": 100,
                        "place": 1,
                    },
                    {
                        "match_id": 1,
                        "match_time": "2000-01-01T00:00:00Z",
                        "offset": 1,
                        "op": 0,
                        "player_id": "Bob",
                        "score": 80,
                        "place": 2,
                    }
                ],
                "dataFormat": "JsonAoS"
            }),
            tail_result
        );
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
