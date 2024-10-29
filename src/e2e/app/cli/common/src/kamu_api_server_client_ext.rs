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
use opendatafabric::{AccountName, DatasetAlias, DatasetName};
use reqwest::{Method, StatusCode};

use crate::{KamuApiServerClient, RequestBody};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// <https://github.com/kamu-data/kamu-cli/blob/master/examples/leaderboard/player-scores.yaml>
pub const DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR: &str = indoc::indoc!(
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
);

/// Based on <https://github.com/kamu-data/kamu-cli/blob/master/examples/leaderboard/leaderboard.yaml>
pub const DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR: &str = indoc::indoc!(
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
            engine: datafusion
            queries:
              - query: |
                  SELECT ROW_NUMBER() OVER (PARTITION BY 1 ORDER BY score DESC) AS place,
                         match_time,
                         match_id,
                         player_id,
                         score
                  FROM player_scores
                  LIMIT 2
        - kind: SetVocab
          eventTimeColumn: match_time
    "#
);

lazy_static! {
    /// <https://github.com/kamu-data/kamu-cli/blob/master/examples/leaderboard/player-scores.yaml>
    pub static ref DATASET_ROOT_PLAYER_SCORES_SNAPSHOT: String = {
        DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR
            .escape_default()
            .to_string()
    };

    /// <https://github.com/kamu-data/kamu-cli/blob/master/examples/leaderboard/leaderboard.yaml>
    pub static ref DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT: String = {
        DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR
            .escape_default()
            .to_string()
    };
}

/// <https://raw.githubusercontent.com/kamu-data/kamu-cli/refs/heads/master/examples/leaderboard/data/1.ndjson>
pub const DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1: &str = indoc::indoc!(
    r#"
    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Alice", "score": 100}
    {"match_time": "2000-01-01", "match_id": 1, "player_id": "Bob", "score": 80}
    "#
);

/// <https://raw.githubusercontent.com/kamu-data/kamu-cli/refs/heads/master/examples/leaderboard/data/2.ndjson>
pub const DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_2: &str = indoc::indoc!(
    r#"
    {"match_time": "2000-01-02", "match_id": 2, "player_id": "Alice", "score": 70}
    {"match_time": "2000-01-02", "match_id": 2, "player_id": "Charlie", "score": 90}
    "#
);

/// <https://raw.githubusercontent.com/kamu-data/kamu-cli/refs/heads/master/examples/leaderboard/data/3.ndjson>
pub const DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_3: &str = indoc::indoc!(
    r#"
    {"match_time": "2000-01-03", "match_id": 3, "player_id": "Bob", "score": 60}
    {"match_time": "2000-01-03", "match_id": 3, "player_id": "Charlie", "score": 110}
    "#
);

pub const E2E_USER_ACCOUNT_NAME_STR: &str = "e2e-user";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type AccessToken = String;
pub type DatasetId = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuApiServerClientExt {
    async fn login_as_kamu(&self) -> AccessToken;

    async fn login_as_e2e_user(&self) -> AccessToken;

    // TODO: also return alias, after solving this bug:
    //       https://github.com/kamu-data/kamu-cli/issues/891
    async fn create_dataset(&self, dataset_snapshot_yaml: &str, token: &AccessToken) -> DatasetId;

    async fn create_player_scores_dataset(&self, token: &AccessToken) -> DatasetId;

    async fn create_player_scores_dataset_with_data(
        &self,
        token: &AccessToken,
        account_name_maybe: Option<AccountName>,
    ) -> DatasetId;

    async fn create_leaderboard(&self, token: &AccessToken) -> DatasetId;

    async fn ingest_data(
        &self,
        dataset_alias: &DatasetAlias,
        data: RequestBody,
        token: &AccessToken,
    );
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
        self.create_dataset(&DATASET_ROOT_PLAYER_SCORES_SNAPSHOT, token)
            .await
    }

    async fn create_player_scores_dataset_with_data(
        &self,
        token: &AccessToken,
        account_name_maybe: Option<AccountName>,
    ) -> DatasetId {
        let dataset_id = self.create_player_scores_dataset(token).await;

        // TODO: Use the alias from the reply, after fixing the bug:
        //       https://github.com/kamu-data/kamu-cli/issues/891
        let dataset_alias = DatasetAlias::new(
            account_name_maybe,
            DatasetName::new_unchecked("player-scores"),
        );

        self.ingest_data(
            &dataset_alias,
            RequestBody::NdJson(DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into()),
            token,
        )
        .await;

        dataset_id
    }

    async fn create_leaderboard(&self, token: &AccessToken) -> DatasetId {
        self.create_dataset(&DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT, token)
            .await
    }

    async fn ingest_data(
        &self,
        dataset_alias: &DatasetAlias,
        data: RequestBody,
        token: &AccessToken,
    ) {
        let endpoint = format!("{dataset_alias}/ingest");

        self.rest_api_call_assert(
            Some(token.clone()),
            Method::POST,
            endpoint.as_str(),
            Some(data),
            StatusCode::OK,
            None,
        )
        .await;
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
