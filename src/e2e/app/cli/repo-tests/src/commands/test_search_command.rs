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
use kamu_cli_puppet::KamuCliPuppet;
use opendatafabric::*;
use reqwest::Url;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_multi_user(kamu_node_api_client: KamuApiServerClient) {
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

    add_kamu_node_repo_to_workspace(&kamu_node_api_client, &kamu).await;

    assert_search(
        &kamu,
        "player",
        indoc::indoc!(
            r#"
            ┌───────┬──────┬─────────────┬────────┬─────────┬──────┐
            │ Alias │ Kind │ Description │ Blocks │ Records │ Size │
            ├───────┼──────┼─────────────┼────────┼─────────┼──────┤
            │       │      │             │        │         │      │
            └───────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        ),
    )
    .await;

    let e2e_user_token = kamu_node_api_client.login_as_e2e_user().await;

    kamu_node_api_client
        .create_player_scores_dataset(&e2e_user_token)
        .await;

    assert_search(
        &kamu,
        "player",
        indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        ),
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

    assert_search(
        &kamu,
        "player",
        indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │   Size   │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      5 │       2 │ 1.63 KiB │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────────┘
            "#
        ),
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

    assert_search(
        &kamu,
        "player",
        indoc::indoc!(
            r#"
            ┌───────────────────────────────────────┬────────────┬─────────────┬────────┬─────────┬──────────┐
            │                 Alias                 │    Kind    │ Description │ Blocks │ Records │   Size   │
            ├───────────────────────────────────────┼────────────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-leaderboard │ Derivative │ -           │      3 │       - │        - │
            │ kamu-node/e2e-user/player-scores      │    Root    │ -           │      5 │       2 │ 1.63 KiB │
            └───────────────────────────────────────┴────────────┴─────────────┴────────┴─────────┴──────────┘
            "#
        ),
    )
    .await;

    let kamu_token = kamu_node_api_client.login_as_kamu().await;

    kamu_node_api_client
        .create_player_scores_dataset(&kamu_token)
        .await;

    assert_search(
        &kamu,
        "player",
        indoc::indoc!(
            r#"
            ┌───────────────────────────────────────┬────────────┬─────────────┬────────┬─────────┬──────────┐
            │                 Alias                 │    Kind    │ Description │ Blocks │ Records │   Size   │
            ├───────────────────────────────────────┼────────────┼─────────────┼────────┼─────────┼──────────┤
            │ kamu-node/e2e-user/player-leaderboard │ Derivative │ -           │      3 │       - │        - │
            │ kamu-node/e2e-user/player-scores      │    Root    │ -           │      5 │       2 │ 1.63 KiB │
            │ kamu-node/kamu/player-scores          │    Root    │ -           │      3 │       - │        - │
            └───────────────────────────────────────┴────────────┴─────────────┴────────┴─────────┴──────────┘
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub async fn test_search_by_name(kamu_node_api_client: KamuApiServerClient) {
    let kamu = KamuCliPuppet::new_workspace_tmp().await;

    add_kamu_node_repo_to_workspace(&kamu_node_api_client, &kamu).await;

    let e2e_user_token = kamu_node_api_client.login_as_e2e_user().await;

    kamu_node_api_client
        .create_player_scores_dataset(&e2e_user_token)
        .await;

    kamu_node_api_client
        .create_leaderboard(&e2e_user_token)
        .await;

    assert_search(
        &kamu,
        "player",
        indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        ),
    )
    .await;

    assert_search(
        &kamu,
        "scores",
        indoc::indoc!(
            r#"
            ┌──────────────────────────────────┬──────┬─────────────┬────────┬─────────┬──────┐
            │              Alias               │ Kind │ Description │ Blocks │ Records │ Size │
            ├──────────────────────────────────┼──────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/player-scores │ Root │ -           │      3 │       - │    - │
            └──────────────────────────────────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        ),
    )
    .await;

    assert_search(
        &kamu,
        "not-relevant-query",
        indoc::indoc!(
            r#"
            ┌───────┬──────┬─────────────┬────────┬─────────┬──────┐
            │ Alias │ Kind │ Description │ Blocks │ Records │ Size │
            ├───────┼──────┼─────────────┼────────┼─────────┼──────┤
            │       │      │             │        │         │      │
            └───────┴──────┴─────────────┴────────┴─────────┴──────┘
            "#
        ),
    )
    .await;

    assert_search(
        &kamu,
        "lead",
        indoc::indoc!(
            r#"
            ┌────────────────────────────────┬────────────┬─────────────┬────────┬─────────┬──────┐
            │             Alias              │    Kind    │ Description │ Blocks │ Records │ Size │
            ├────────────────────────────────┼────────────┼─────────────┼────────┼─────────┼──────┤
            │ kamu-node/e2e-user/leaderboard │ Derivative │ -           │      3 │       - │    - │
            └────────────────────────────────┴────────────┴─────────────┴────────┴─────────┴──────┘
            "#
        ),
    )
    .await;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn add_kamu_node_repo_to_workspace(
    kamu_node_api_client: &KamuApiServerClient,
    kamu: &KamuCliPuppet,
) {
    let http_repo = {
        let mut url = Url::parse("odf+http://host").unwrap();
        let base_url = kamu_node_api_client.get_base_url();
        url.set_host(base_url.host_str()).unwrap();
        url.set_port(base_url.port()).unwrap();
        url
    };

    let assert = kamu
        .execute(["repo", "add", "kamu-node", http_repo.as_str()])
        .await
        .success();

    let stderr = std::str::from_utf8(&assert.get_output().stderr).unwrap();

    assert!(
        stderr.contains("Added: kamu-node"),
        "Unexpected output:\n{stderr}",
    );
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn assert_search(kamu: &KamuCliPuppet, query: &str, expected_table_output: &str) {
    let assert = kamu
        .execute(["search", query, "--output-format", "table"])
        .await
        .success();

    let stdout = std::str::from_utf8(&assert.get_output().stdout).unwrap();

    pretty_assertions::assert_eq!(stdout, expected_table_output);
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
