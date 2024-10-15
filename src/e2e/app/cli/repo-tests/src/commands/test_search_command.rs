// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_cli_e2e_common::{
    KamuApiServerClient,
    KamuApiServerClientExt,
    RequestBody,
    DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1,
};
use kamu_cli_puppet::extensions::KamuCliPuppetExt;
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_multi_user(kamu_node_api_client: KamuApiServerClient) {
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

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

    let e2e_user_token = kamu_node_api_client.login_as_e2e_user().await;

    kamu_node_api_client
        .create_player_scores_dataset(&e2e_user_token)
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

    let player_scores_alias = DatasetAlias::new(
        Some(AccountName::new_unchecked("e2e-user")),
        DatasetName::new_unchecked("player-scores"),
    );

    kamu_node_api_client
        .ingest_data(
            &player_scores_alias,
            RequestBody::NdJson(DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into()),
            &e2e_user_token,
        )
        .await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │   Size   │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      5 │       2 │ 1.63 KiB │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    // The same as DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT, but contains the word
    // "player" in the name so that it can be found together with "player-scores"
    let dataset_derivative_player_leaderboard_snapshot = indoc::indoc!(
        r#"
        kind: DatasetSnapshot
        version: 1
        content:
          name: player-leaderboard
          kind: Derivative
          metadata:
            - kind: SetTransform
              inputs:
                - datasetRef: player-scores
                  alias: player_scores
              transform:
                kind: Sql
                engine: risingwave
                queries:
                  - alias: leaderboard
                    query: |
                      create materialized view leaderboard as
                      select
                        *
                      from (
                        select
                          row_number() over (partition by 1 order by score desc) as place,
                          match_time,
                          match_id,
                          player_id,
                          score
                        from player_scores
                      )
                      where place <= 2
                  - query: |
                      select * from leaderboard
            - kind: SetVocab
              eventTimeColumn: match_time
        "#
    )
    .escape_default()
    .to_string();

    kamu_node_api_client
        .create_dataset(
            &dataset_derivative_player_leaderboard_snapshot,
            &e2e_user_token,
        )
        .await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌───────────────────────────────────────┬────────────┬─────────────┬────────┬─────────┬──────────┐
            │                 Alias                 │    Kind    │ Description │ Blocks │ Records │   Size   │
            ├───────────────────────────────────────┼────────────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-leaderboard │ Derivative │ -           │      3 │       - │        - │
            │ kamu-node/e2e-user/player-scores      │    Root    │ -           │      5 │       2 │ 1.63 KiB │
            └───────────────────────────────────────┴────────────┴─────────────┴────────┴─────────┴──────────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;

    let kamu_token = kamu_node_api_client.login_as_kamu().await;

    kamu_node_api_client
        .create_player_scores_dataset(&kamu_token)
        .await;

    kamu.assert_success_command_execution(
        ["search", "player", "--output-format", "table"],
        Some(indoc::indoc!(
            r#"
            ┌───────────────────────────────────────┬────────────┬─────────────┬────────┬─────────┬──────────┐
            │                 Alias                 │    Kind    │ Description │ Blocks │ Records │   Size   │
            ├───────────────────────────────────────┼────────────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-leaderboard │ Derivative │ -           │      3 │       - │        - │
            │ kamu-node/e2e-user/player-scores      │    Root    │ -           │      5 │       2 │ 1.63 KiB │
            │ kamu-node/kamu/player-scores          │    Root    │ -           │      3 │       - │        - │
            └───────────────────────────────────────┴────────────┴─────────────┴────────┴─────────┴──────────┘
            "#
        )),
        None::<Vec<&str>>,
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_by_name(kamu_node_api_client: KamuApiServerClient) {
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

    add_repo_to_workspace(&kamu_node_api_client, &kamu, "kamu-node").await;

    let e2e_user_token = kamu_node_api_client.login_as_e2e_user().await;

    kamu_node_api_client
        .create_player_scores_dataset(&e2e_user_token)
        .await;

    kamu_node_api_client
        .create_leaderboard(&e2e_user_token)
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

pub async fn test_search_by_repo(kamu_node_api_client: KamuApiServerClient) {
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

    // As a test, add two repos pointing to the same node
    add_repo_to_workspace(&kamu_node_api_client, &kamu, "kamu-node").await;
    add_repo_to_workspace(&kamu_node_api_client, &kamu, "acme-org-node").await;

    let e2e_user_token = kamu_node_api_client.login_as_e2e_user().await;

    kamu_node_api_client
        .create_player_scores_dataset(&e2e_user_token)
        .await;

    kamu_node_api_client
        .create_leaderboard(&e2e_user_token)
        .await;

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
            kamu_node_api_client.get_node_url().as_str(),
        ],
        None,
        Some([format!("Added: {repo_name}").as_str()]),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
