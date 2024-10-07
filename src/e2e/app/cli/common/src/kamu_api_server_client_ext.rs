// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use lazy_static::lazy_static;
use reqwest::{Method, StatusCode};

use crate::{KamuApiServerClient, RequestBody};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

lazy_static! {
    // https://github.com/kamu-data/kamu-cli/blob/master/examples/leaderboard/player-scores.yaml
    pub static ref DATASET_ROOT_PLAYER_SCORES_SHAPSHOT: String = {
        indoc::indoc!(
            r#"
            kind: DatasetSnapshot
            version: 1
            content:
              name: player-scores
              kind: Root
              metadata:
                - kind: AddPushSource
                  sourceName: default
                  read:
                    kind: NdJson
                    schema:
                      - "match_time TIMESTAMP"
                      - "match_id BIGINT"
                      - "player_id STRING"
                      - "score BIGINT"
                  merge:
                    kind: Ledger
                    primaryKey:
                      - match_id
                      - player_id
                - kind: SetVocab
                  eventTimeColumn: match_time
            "#
        )
        .escape_default()
        .to_string()
    };

    // https://github.com/kamu-data/kamu-cli/blob/master/examples/leaderboard/leaderboard.yaml
    pub static ref DATASET_DERIVATIVE_LEADERBOARD_SHAPSHOT: String = {
        indoc::indoc!(
            r#"
            kind: DatasetSnapshot
            version: 1
            content:
              name: leaderboard
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
                        # Note we are using explicit `crate materialized view` statement below
                        # because RW does not currently support Top-N queries directly on sinks.
                        #
                        # Note `partition by 1` is currently required by RW engine
                        # See: https://docs.risingwave.com/docs/current/window-functions/#syntax
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
        .to_string()
    };
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type AccessToken = String;
pub type DatasetId = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuApiServerClientExt {
    async fn login_as_kamu(&self) -> AccessToken;
    async fn login_as_e2e_user(&self) -> AccessToken;
    async fn create_dataset(&self, dataset_snapshot_yaml: &str, token: &AccessToken) -> DatasetId;
    async fn create_player_scores_dataset(&self, token: &AccessToken) -> DatasetId;
    async fn create_player_scores_dataset_with_data(&self, token: &AccessToken) -> DatasetId;
    async fn create_leaderboard(&self, token: &AccessToken) -> DatasetId;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl KamuApiServerClientExt for KamuApiServerClient {
    async fn login_as_kamu(&self) -> AccessToken {
        login_as_kamu(
            self,
            indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: "password", loginCredentialsJson: "{\"login\":\"kamu\",\"password\":\"kamu\"}") {
                      accessToken
                    }
                  }
                }
                "#,
            )
        ).await
    }

    async fn login_as_e2e_user(&self) -> AccessToken {
        // We are using DummyOAuthGithub, so the loginCredentialsJson can be arbitrary
        login_as_kamu(
            self,
            indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: "oauth_github", loginCredentialsJson: "") {
                      accessToken
                    }
                  }
                }
                "#,
            ),
        )
        .await
    }

    async fn create_dataset(&self, dataset_snapshot_yaml: &str, token: &AccessToken) -> DatasetId {
        let create_response = self
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    mutation {
                      datasets {
                        createFromSnapshot(snapshot: "<snapshot>", snapshotFormat: YAML) {
                          message
                          ... on CreateDatasetResultSuccess {
                            dataset {
                              id
                            }
                          }
                        }
                      }
                    }
                    "#,
                )
                .replace("<snapshot>", dataset_snapshot_yaml)
                .as_str(),
                Some(token.clone()),
            )
            .await;

        assert_eq!(
            create_response["datasets"]["createFromSnapshot"]["message"].as_str(),
            Some("Success")
        );

        let dataset_id = create_response["datasets"]["createFromSnapshot"]["dataset"]["id"]
            .as_str()
            .map(ToOwned::to_owned)
            .unwrap();

        dataset_id
    }

    async fn create_player_scores_dataset(&self, token: &AccessToken) -> DatasetId {
        self.create_dataset(&DATASET_ROOT_PLAYER_SCORES_SHAPSHOT, token)
            .await
    }

    async fn create_player_scores_dataset_with_data(&self, token: &AccessToken) -> DatasetId {
        let dataset_id = self.create_player_scores_dataset(token).await;

        self.rest_api_call_assert(
            Some(token.clone()),
            Method::POST,
            "player-scores/ingest",
            Some(RequestBody::NdJson(
                indoc::indoc!(
                    r#"
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
                    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
                    "#,
                )
                .into(),
            )),
            StatusCode::OK,
            None,
        )
        .await;

        dataset_id
    }

    async fn create_leaderboard(&self, token: &AccessToken) -> DatasetId {
        self.create_dataset(&DATASET_DERIVATIVE_LEADERBOARD_SHAPSHOT, token)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helper functions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

async fn login_as_kamu(
    kamu_api_server_client: &KamuApiServerClient,
    login_request: &str,
) -> AccessToken {
    let login_response = kamu_api_server_client
        .graphql_api_call(login_request, None)
        .await;
    let access_token = login_response["auth"]["login"]["accessToken"]
        .as_str()
        .map(ToOwned::to_owned)
        .unwrap();

    access_token
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
