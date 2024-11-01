// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use convert_case::{Case, Casing};
use kamu_flow_system::{DatasetFlowType, FlowID};
use lazy_static::lazy_static;
use opendatafabric as odf;
use reqwest::{Method, StatusCode, Url};
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;

use crate::{AccessToken, KamuApiServerClient, RequestBody};

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

pub struct CreateDatasetResponse {
    pub dataset_id: odf::DatasetID,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuApiServerClientExt {
    fn auth(&mut self) -> AuthApi<'_>;

    fn data(&self) -> DataApi;

    fn dataset(&self) -> DatasetApi;

    fn flow(&self) -> FlowApi;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl KamuApiServerClientExt for KamuApiServerClient {
    fn auth(&mut self) -> AuthApi<'_> {
        AuthApi { client: self }
    }

    fn data(&self) -> DataApi {
        DataApi { client: self }
    }

    fn dataset(&self) -> DatasetApi {
        DatasetApi { client: self }
    }

    fn flow(&self) -> FlowApi {
        FlowApi { client: self }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Auth
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AuthApi<'a> {
    client: &'a mut KamuApiServerClient,
}

impl AuthApi<'_> {
    pub async fn login_as_kamu(&mut self) -> AccessToken {
        self.login(
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

    pub async fn login_as_e2e_user(&mut self) -> AccessToken {
        // We are using DummyOAuthGithub, so the loginCredentialsJson can be arbitrary
        self.login(indoc::indoc!(
            r#"
            mutation {
              auth {
                login(loginMethod: "oauth_github", loginCredentialsJson: "") {
                  accessToken
                }
              }
            }
            "#,
        ))
        .await
    }

    async fn login(&mut self, login_request: &str) -> AccessToken {
        let login_response = self.client.graphql_api_call(login_request).await;
        let access_token = login_response["auth"]["login"]["accessToken"]
            .as_str()
            .map(ToOwned::to_owned)
            .unwrap();

        self.client.set_token(Some(access_token.clone()));

        access_token
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Data
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DataApi<'a> {
    client: &'a KamuApiServerClient,
}

impl DataApi<'_> {
    pub async fn query(&self, query: &str) -> String {
        let response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      data {
                        query(
                          query: """
                          <query>
                          """,
                          queryDialect: SQL_DATA_FUSION,
                          dataFormat: CSV
                        ) {
                          __typename
                          ... on DataQueryResultSuccess {
                            data {
                              content
                            }
                          }
                          ... on DataQueryResultError {
                            errorKind
                            errorMessage
                          }
                        }
                      }
                    }
                    "#,
                )
                .replace("<query>", query)
                .as_str(),
            )
            .await;
        let query_node = &response["data"]["query"];

        assert_eq!(
            query_node["__typename"].as_str(),
            Some("DataQueryResultSuccess"),
            "{}",
            indoc::formatdoc!(
                r#"
                Query:
                {query}
                Unexpected response:
                {query_node:#}
                "#
            )
        );

        let content = query_node["data"]["content"]
            .as_str()
            .map(ToOwned::to_owned)
            .unwrap();

        content
    }

    pub async fn query_player_scores_dataset(&self) -> String {
        // Without unstable "offset" column
        self.query(indoc::indoc!(
            r#"
            SELECT op,
                   system_time,
                   match_time,
                   match_id,
                   player_id,
                   score
            FROM 'player-scores'
            ORDER BY match_id, score, player_id
            "#
        ))
        .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Dataset
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetApi<'a> {
    client: &'a KamuApiServerClient,
}

impl DatasetApi<'_> {
    pub fn get_endpoint(&self, dataset_alias: &odf::DatasetAlias) -> Url {
        let node_url = self.client.get_odf_node_url();

        node_url.join(format!("{dataset_alias}").as_str()).unwrap()
    }

    pub async fn create_empty_dataset(
        &self,
        dataset_kind: odf::DatasetKind,
        dataset_alias: &odf::DatasetAlias,
    ) -> CreateDatasetResponse {
        let create_response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    mutation {
                      datasets {
                        createEmpty(datasetKind: <dataset_kind>, datasetAlias: "<dataset_alias>") {
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
                .replace(
                    "<dataset_kind>",
                    &format!("{dataset_kind:?}").to_uppercase(),
                )
                .replace("<dataset_alias>", &format!("{dataset_alias}"))
                .as_str(),
            )
            .await;

        let create_response_node = &create_response["datasets"]["createEmpty"];

        pretty_assertions::assert_eq!(Some("Success"), create_response_node["message"].as_str());

        let dataset_id_as_str = create_response_node["dataset"]["id"].as_str().unwrap();

        CreateDatasetResponse {
            dataset_id: odf::DatasetID::from_did_str(dataset_id_as_str).unwrap(),
        }
    }

    pub async fn create_dataset(&self, dataset_snapshot_yaml: &str) -> CreateDatasetResponse {
        let create_response = self
            .client
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
            )
            .await;

        let create_response_node = &create_response["datasets"]["createFromSnapshot"];

        pretty_assertions::assert_eq!(Some("Success"), create_response_node["message"].as_str());

        let dataset_id_as_str = create_response_node["dataset"]["id"].as_str().unwrap();

        CreateDatasetResponse {
            dataset_id: odf::DatasetID::from_did_str(dataset_id_as_str).unwrap(),
        }
    }

    pub async fn create_player_scores_dataset(&self) -> CreateDatasetResponse {
        self.create_dataset(&DATASET_ROOT_PLAYER_SCORES_SNAPSHOT)
            .await
    }

    pub async fn create_player_scores_dataset_with_data(
        &self,
        account_name_maybe: Option<odf::AccountName>,
    ) -> CreateDatasetResponse {
        let create_response = self.create_player_scores_dataset().await;

        // TODO: Use the alias from the reply, after fixing the bug:
        //       https://github.com/kamu-data/kamu-cli/issues/891
        let dataset_alias = odf::DatasetAlias::new(
            account_name_maybe,
            odf::DatasetName::new_unchecked("player-scores"),
        );

        self.ingest_data(
            &dataset_alias,
            RequestBody::NdJson(DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into()),
        )
        .await;

        create_response
    }

    pub async fn create_leaderboard(&self) -> CreateDatasetResponse {
        self.create_dataset(&DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT)
            .await
    }

    pub async fn ingest_data(&self, dataset_alias: &odf::DatasetAlias, data: RequestBody) {
        let endpoint = format!("{dataset_alias}/ingest");

        self.client
            .rest_api_call_assert(
                Method::POST,
                endpoint.as_str(),
                Some(data),
                StatusCode::OK,
                None,
            )
            .await;
    }

    pub async fn tail_data(&self, dataset_id: &odf::DatasetID) -> String {
        let tail_response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      datasets {
                        byId(
                          datasetId: "<dataset_id>"
                        ) {
                          data {
                            tail(dataFormat: "CSV") {
                              ... on DataQueryResultSuccess {
                                data {
                                  content
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                    "#,
                )
                .replace("<dataset_id>", &dataset_id.as_did_str().to_stack_string())
                .as_str(),
            )
            .await;

        let content = tail_response["datasets"]["byId"]["data"]["tail"]["data"]["content"]
            .as_str()
            .map(ToOwned::to_owned)
            .unwrap();

        content
    }

    pub async fn blocks(&self, dataset_id: &odf::DatasetID) -> DatasetBlocksResponse {
        let response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      datasets {
                        byId(datasetId: "<dataset_id>") {
                          metadata {
                            chain {
                              blocks {
                                edges {
                                  node {
                                    blockHash
                                    prevBlockHash
                                    systemTime
                                    sequenceNumber
                                    event {
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
                .replace("<dataset_id>", &dataset_id.as_did_str().to_stack_string())
                .as_str(),
            )
            .await;

        let blocks = response["datasets"]["byId"]["metadata"]["chain"]["blocks"]["edges"]
            .as_array()
            .unwrap()
            .iter()
            .map(|edge_node| {
                let node = &edge_node["node"];

                let block_hash_as_str = node["blockHash"].as_str().unwrap();
                let maybe_prev_block_hash_as_str = node["prevBlockHash"].as_str();
                let system_time_as_str = node["systemTime"].as_str().unwrap();
                let sequence_number = node["sequenceNumber"].as_u64().unwrap();
                let event = match node["event"]["__typename"].as_str().unwrap() {
                    "AddData" => odf::MetadataEventTypeFlags::ADD_DATA,
                    "ExecuteTransform" => odf::MetadataEventTypeFlags::EXECUTE_TRANSFORM,
                    "Seed" => odf::MetadataEventTypeFlags::SEED,
                    "SetPollingSource" => odf::MetadataEventTypeFlags::SET_POLLING_SOURCE,
                    "SetTransform" => odf::MetadataEventTypeFlags::SET_TRANSFORM,
                    "SetVocab" => odf::MetadataEventTypeFlags::SET_VOCAB,
                    "SetAttachments" => odf::MetadataEventTypeFlags::SET_ATTACHMENTS,
                    "SetInfo" => odf::MetadataEventTypeFlags::SET_INFO,
                    "SetLicense" => odf::MetadataEventTypeFlags::SET_LICENSE,
                    "SetDataSchema" => odf::MetadataEventTypeFlags::SET_DATA_SCHEMA,
                    "AddPushSource" => odf::MetadataEventTypeFlags::ADD_PUSH_SOURCE,
                    "DisablePushSource" => odf::MetadataEventTypeFlags::DISABLE_PUSH_SOURCE,
                    "DisablePollingSource" => odf::MetadataEventTypeFlags::DISABLE_POLLING_SOURCE,
                    unexpected_event => panic!("Unexpected event type: {unexpected_event}"),
                };

                DatasetBlock {
                    block_hash: odf::Multihash::from_multibase(block_hash_as_str).unwrap(),
                    prev_block_hash: maybe_prev_block_hash_as_str
                        .map(|hash| odf::Multihash::from_multibase(hash).unwrap()),
                    system_time: system_time_as_str.parse().unwrap(),
                    sequence_number,
                    event,
                }
            })
            .collect::<Vec<_>>();

        DatasetBlocksResponse { blocks }
    }
}

#[derive(Debug)]
pub struct DatasetBlock {
    pub block_hash: odf::Multihash,
    pub prev_block_hash: Option<odf::Multihash>,
    pub system_time: DateTime<Utc>,
    pub sequence_number: u64,
    pub event: odf::MetadataEventTypeFlags,
}

#[derive(Debug)]
pub struct DatasetBlocksResponse {
    pub blocks: Vec<DatasetBlock>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Flow
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct FlowApi<'a> {
    client: &'a KamuApiServerClient,
}

impl FlowApi<'_> {
    pub async fn trigger(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_flow_type: DatasetFlowType,
    ) -> FlowTriggerResponse {
        let response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    mutation {
                      datasets {
                        byId(datasetId: "<dataset_id>") {
                          flows {
                            runs {
                              triggerFlow(datasetFlowType: <dataset_flow_type>) {
                                message
                                ... on TriggerFlowSuccess {
                                  flow {
                                    flowId
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
                .replace("<dataset_id>", &dataset_id.as_did_str().to_stack_string())
                .replace(
                    "<dataset_flow_type>",
                    &format!("{dataset_flow_type:?}").to_case(Case::ScreamingSnake),
                )
                .as_str(),
            )
            .await;

        let trigger_node = &response["datasets"]["byId"]["flows"]["runs"]["triggerFlow"];
        let message = trigger_node["message"].as_str().unwrap();

        if message == "Success" {
            let flow_id_as_str = trigger_node["flow"]["flowId"].as_str().unwrap();
            let flow_id = flow_id_as_str.parse::<u64>().unwrap();

            FlowTriggerResponse::Success(flow_id.into())
        } else {
            FlowTriggerResponse::Error(message.to_owned())
        }
    }

    pub async fn wait(&self, dataset_id: &odf::DatasetID) {
        let retry_strategy = FixedInterval::from_millis(5_000).take(18); // 1m 30s

        Retry::spawn(retry_strategy, || async {
            let response = self
                .client
                .graphql_api_call(
                    indoc::indoc!(
                        r#"
                        query {
                          datasets {
                            byId(datasetId: "<dataset_id>") {
                              flows {
                                runs {
                                  listFlows {
                                    edges {
                                      node {
                                        status
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
                    .replace("<dataset_id>", &dataset_id.as_did_str().to_stack_string())
                    .as_str(),
                )
                .await;

            let edges = response["datasets"]["byId"]["flows"]["runs"]["listFlows"]["edges"]
                .as_array()
                .unwrap();
            let all_finished = edges.iter().all(|edge| {
                let status = edge["node"]["status"].as_str().unwrap();

                status == "FINISHED"
            });

            if all_finished {
                Ok(())
            } else {
                Err(())
            }
        })
        .await
        .unwrap();
    }
}

#[derive(Debug)]
pub enum FlowTriggerResponse {
    Success(FlowID),
    Error(String),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
