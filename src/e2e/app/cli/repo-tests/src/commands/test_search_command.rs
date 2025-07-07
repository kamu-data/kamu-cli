// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR,
    DATASET_ROOT_PLAYER_NAME,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
    E2E_USER_ACCOUNT_NAME,
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
};
use kamu_cli_puppet::KamuCliPuppet;
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use odf::metadata::testing::MetadataFactory;

use crate::utils::make_logged_clients;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_search_multi_user
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_multi_user_st(kamu_node_api_client: KamuApiServerClient) {
    test_search_multi_user(kamu_node_api_client, false).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_multi_user_mt(kamu_node_api_client: KamuApiServerClient) {
    test_search_multi_user(kamu_node_api_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_search_by_name
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_by_name_st(kamu_node_api_client: KamuApiServerClient) {
    test_search_by_name(kamu_node_api_client, false).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_by_name_mt(kamu_node_api_client: KamuApiServerClient) {
    test_search_by_name(kamu_node_api_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_search_by_repo
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_by_repo_st(kamu_node_api_client: KamuApiServerClient) {
    test_search_by_repo(kamu_node_api_client, false).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_by_repo_mt(kamu_node_api_client: KamuApiServerClient) {
    test_search_by_repo(kamu_node_api_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// test_search_private_dataset
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_private_dataset_st(kamu_node_api_client: KamuApiServerClient) {
    test_search_private_dataset(kamu_node_api_client, false).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_private_dataset_mt(kamu_node_api_client: KamuApiServerClient) {
    test_search_private_dataset(kamu_node_api_client, true).await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Implementations
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_search_multi_user(
    mut kamu_node_api_client: KamuApiServerClient,
    is_workspace_multi_tenant: bool,
) {
    let kamu = KamuCliPuppet::new_workspace_tmp(is_workspace_multi_tenant).await;

    add_repo_to_workspace(&kamu_node_api_client, &kamu, "kamu-node").await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌───────┬──────┬─────────────┬────────┬─────────┬──────┐
            │ Alias │ Kind │ Description │ Blocks │ Records │ Size │
            ├───────┼──────┼─────────────┼────────┼─────────┼──────┤
            │       │      │             │        │         │      │
            └───────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu_node_api_client.auth().login_as_e2e_user().await;

    kamu_node_api_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    let player_scores_alias = odf::DatasetAlias::new(
        Some(E2E_USER_ACCOUNT_NAME.clone()),
        DATASET_ROOT_PLAYER_NAME.clone(),
    );

    kamu_node_api_client
        .dataset()
        .ingest_data(
            &player_scores_alias,
            RequestBody::NdJson(DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into()),
        )
        .await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │   Size   │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      5 │       2 │ 2.42 KiB │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    // Updating the name for ease of search so that it can be found
    // together with "player-scores".
    let dataset_derivative_player_leaderboard_snapshot =
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR
            .escape_default()
            .to_string()
            .replace("leaderboard", "player-leaderboard");

    kamu_node_api_client
        .dataset()
        .create_dataset(&dataset_derivative_player_leaderboard_snapshot)
        .await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌───────────────────────────────────────┬────────────┬─────────────┬────────┬─────────┬──────────┐
            │                 Alias                 │    Kind    │ Description │ Blocks │ Records │   Size   │
            ├───────────────────────────────────────┼────────────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-leaderboard │ Derivative │ -           │      3 │       - │        - │
            │ kamu-node/e2e-user/player-scores      │    Root    │ -           │      5 │       2 │ 2.42 KiB │
            └───────────────────────────────────────┴────────────┴─────────────┴────────┴─────────┴──────────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu_node_api_client.auth().login_as_kamu().await;

    kamu_node_api_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌───────────────────────────────────────┬────────────┬─────────────┬────────┬─────────┬──────────┐
            │                 Alias                 │    Kind    │ Description │ Blocks │ Records │   Size   │
            ├───────────────────────────────────────┼────────────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-leaderboard │ Derivative │ -           │      3 │       - │        - │
            │ kamu-node/e2e-user/player-scores      │    Root    │ -           │      5 │       2 │ 2.42 KiB │
            │ kamu-node/kamu/player-scores          │    Root    │ -           │      3 │       - │        - │
            └───────────────────────────────────────┴────────────┴─────────────┴────────┴─────────┴──────────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_search_by_name(
    mut kamu_node_api_client: KamuApiServerClient,
    is_workspace_multi_tenant: bool,
) {
    let kamu = KamuCliPuppet::new_workspace_tmp(is_workspace_multi_tenant).await;

    add_repo_to_workspace(&kamu_node_api_client, &kamu, "kamu-node").await;

    kamu_node_api_client.auth().login_as_e2e_user().await;

    kamu_node_api_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    kamu_node_api_client.dataset().create_leaderboard().await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        ["search", "scores", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        ["search", "not-relevant-query", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌───────┬──────┬─────────────┬────────┬─────────┬──────┐
            │ Alias │ Kind │ Description │ Blocks │ Records │ Size │
            ├───────┼──────┼─────────────┼────────┼─────────┼──────┤
            │       │      │             │        │         │      │
            └───────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        ["search", "lead", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌────────────────────────────────┬────────────┬─────────────┬────────┬─────────┬──────┐
            │             Alias              │    Kind    │ Description │ Blocks │ Records │ Size │
            ├────────────────────────────────┼────────────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/leaderboard │ Derivative │ -           │      3 │       - │    - │
            └────────────────────────────────┴────────────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_search_by_repo(
    mut kamu_node_api_client: KamuApiServerClient,
    is_workspace_multi_tenant: bool,
) {
    let kamu = KamuCliPuppet::new_workspace_tmp(is_workspace_multi_tenant).await;

    // As a test, add two repos pointing to the same node
    add_repo_to_workspace(&kamu_node_api_client, &kamu, "kamu-node").await;
    add_repo_to_workspace(&kamu_node_api_client, &kamu, "acme-org-node").await;

    kamu_node_api_client.auth().login_as_e2e_user().await;

    kamu_node_api_client
        .dataset()
        .create_player_scores_dataset()
        .await;

    kamu_node_api_client.dataset().create_leaderboard().await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌──────────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │                Alias                 │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ acme-org-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            │ kamu-node/e2e-user/player-scores     │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        [
            "search",
            "player",
            "--repo",
            "acme-org-node",
            "--output-format",
            "table",
        ],
        Some(indoc::indoc!(
            r#"
            ┌──────────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │                Alias                 │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ acme-org-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    kamu.assert_success_command_execution(
        [
            "search",
            "player",
            "--repo",
            "kamu-node",
            "--output-format",
            "table",
        ],
        Some(indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn test_search_private_dataset(
    anonymous: KamuApiServerClient,
    is_workspace_multi_tenant: bool,
) {
    let [alice, _bob] = make_logged_clients(&anonymous, ["alice", "bob"]).await;

    // Alice has two datasets, one private and one public
    alice
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("public-dataset")
                .build(),
            odf::DatasetVisibility::Public,
        )
        .await;
    alice
        .dataset()
        .create_dataset_from_snapshot_with_visibility(
            MetadataFactory::dataset_snapshot()
                .name("private-dataset")
                .build(),
            odf::DatasetVisibility::Private,
        )
        .await;

    let kamu_node_url = anonymous.get_base_url().as_str();

    // As an owner, she can find all the datasets
    {
        let kamu_workspace_alice =
            KamuCliPuppet::new_workspace_tmp(is_workspace_multi_tenant).await;

        kamu_workspace_alice
            .assert_success_command_execution(
                [
                    "login",
                    "--repo-name",
                    "kamu-node",
                    "password",
                    "alice",
                    "test#alice",
                    kamu_node_url,
                ],
                None,
                Some([format!("Login successful: {kamu_node_url}").as_str()]),
            )
            .await;

        kamu_workspace_alice
            .assert_success_command_execution(
                ["search", "dataset", "--output-format", "table"],
                Some(indoc::indoc!(
                    r#"
                ┌─────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
                │              Alias              │ Kind │ Description │ Blocks │ Records │ Size │
                ├─────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
                │ kamu-node/alice/private-dataset │ Root │ -           │      1 │       - │    - │
                │ kamu-node/alice/public-dataset  │ Root │ -           │      1 │       - │    - │
                └─────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
                "#
                )),
                None::<Vec<&str>>,
            )
            .await;
    }

    // Bob only finds the public dataset
    {
        let kamu_workspace_bob = KamuCliPuppet::new_workspace_tmp(is_workspace_multi_tenant).await;

        kamu_workspace_bob
            .assert_success_command_execution(
                [
                    "login",
                    "--repo-name",
                    "kamu-node",
                    "password",
                    "bob",
                    "test#bob",
                    kamu_node_url,
                ],
                None,
                Some([format!("Login successful: {kamu_node_url}").as_str()]),
            )
            .await;

        kamu_workspace_bob
            .assert_success_command_execution(
                ["search", "dataset", "--output-format", "table"],
                Some(indoc::indoc!(
                    r#"
                    ┌────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
                    │             Alias              │ Kind │ Description │ Blocks │ Records │ Size │
                    ├────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
                    │ kamu-node/alice/public-dataset │ Root │ -           │      1 │       - │    - │
                    └────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
                    "#
                )),
                None::<Vec<&str>>,
            )
            .await;
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn add_repo_to_workspace(
    kamu_node_api_client: &KamuApiServerClient,
    kamu: &KamuCliPuppet,
    repo_name: &str,
) {
    kamu.assert_success_command_execution(
        [
            "repo",
            "add",
            repo_name,
            kamu_node_api_client.get_odf_node_url().as_str(),
        ],
        None,
        Some([format!("Added: {repo_name}").as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
