// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;
use std::sync::LazyLock;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use convert_case::{Case, Casing};
use http_common::comma_separated::CommaSeparatedSet;
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use kamu_accounts_services::PREDEFINED_DEVICE_CODE_UUID;
use kamu_adapter_graphql::traits::ResponseExt;
use kamu_adapter_http::data::metadata_handler::{
    DatasetMetadataParams,
    DatasetMetadataResponse,
    Include as MetadataInclude,
};
use kamu_adapter_http::data::query_types::{QueryRequest, QueryResponse};
use kamu_adapter_http::data::verify_types::{VerifyRequest, VerifyResponse};
use kamu_adapter_http::general::{AccountResponse, DatasetInfoResponse, NodeInfoResponse};
use kamu_adapter_http::platform::{LoginRequestBody, PlatformFileUploadQuery, UploadContext};
use kamu_auth_rebac::AccountToDatasetRelation;
use kamu_flow_system::{DatasetFlowType, FlowID};
use reqwest::{Method, StatusCode, Url};
use serde::Deserialize;
use thiserror::Error;
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;

use crate::kamu_api_server_client::LoggedInUser;
use crate::{AccessToken, GraphQLResponseExt, KamuApiServerClient, RequestBody};

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

/// <https://github.com/kamu-data/kamu-cli/blob/master/examples/leaderboard/player-scores.yaml>
pub static DATASET_ROOT_PLAYER_SCORES_SNAPSHOT: LazyLock<String> = LazyLock::new(|| {
    DATASET_ROOT_PLAYER_SCORES_SNAPSHOT_STR
        .escape_default()
        .to_string()
});

pub static DATASET_ROOT_PLAYER_NAME: LazyLock<odf::DatasetName> =
    LazyLock::new(|| odf::DatasetName::new_unchecked("player-scores"));

/// <https://github.com/kamu-data/kamu-cli/blob/master/examples/leaderboard/leaderboard.yaml>
pub static DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT: LazyLock<String> = LazyLock::new(|| {
    DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT_STR
        .escape_default()
        .to_string()
});

pub static DATASET_DERIVATIVE_LEADERBOARD_NAME: LazyLock<odf::DatasetName> =
    LazyLock::new(|| odf::DatasetName::new_unchecked("leaderboard"));

pub static E2E_USER_ACCOUNT_NAME: LazyLock<odf::AccountName> =
    LazyLock::new(|| odf::AccountName::new_unchecked(E2E_USER_ACCOUNT_NAME_STR));

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

pub const DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_4: &str = indoc::indoc!(
    r#"
    {"match_time": "2000-01-04", "match_id": 4, "player_id": "Bob", "score": 120}
    {"match_time": "2000-01-04", "match_id": 4, "player_id": "Alice", "score": 50}
    "#
);

pub const E2E_USER_ACCOUNT_NAME_STR: &str = "e2e-user";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
pub trait KamuApiServerClientExt {
    fn account(&self) -> AccountApi<'_>;

    fn auth(&mut self) -> AuthApi<'_>;

    fn dataset(&self) -> DatasetApi;

    fn flow(&self) -> FlowApi;

    fn odf_core(&self) -> OdfCoreApi;

    fn odf_transfer(&self) -> OdfTransferApi;

    fn odf_query(&self) -> OdfQuery;

    fn search(&self) -> SearchApi;

    fn swagger(&self) -> SwaggerApi;

    fn upload(&self) -> UploadApi;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait]
impl KamuApiServerClientExt for KamuApiServerClient {
    fn account(&self) -> AccountApi<'_> {
        AccountApi { client: self }
    }

    fn auth(&mut self) -> AuthApi<'_> {
        AuthApi { client: self }
    }

    fn dataset(&self) -> DatasetApi {
        DatasetApi { client: self }
    }

    fn flow(&self) -> FlowApi {
        FlowApi { client: self }
    }

    fn odf_core(&self) -> OdfCoreApi {
        OdfCoreApi { client: self }
    }

    fn odf_transfer(&self) -> OdfTransferApi {
        OdfTransferApi { client: self }
    }

    fn odf_query(&self) -> OdfQuery {
        OdfQuery { client: self }
    }

    fn search(&self) -> SearchApi {
        SearchApi { client: self }
    }

    fn swagger(&self) -> SwaggerApi {
        SwaggerApi { client: self }
    }

    fn upload(&self) -> UploadApi {
        UploadApi { client: self }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Auth
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountApi<'a> {
    client: &'a KamuApiServerClient,
}

impl AccountApi<'_> {
    pub async fn me(&mut self) -> Result<AccountResponse, AccountMeError> {
        let response = self
            .client
            .rest_api_call(Method::GET, "/accounts/me", None)
            .await;

        match response.status() {
            StatusCode::OK => Ok(response.json().await.int_err()?),
            StatusCode::UNAUTHORIZED => Err(AccountMeError::Unauthorized),
            unexpected_status => Err(format!("Unexpected status: {unexpected_status}")
                .int_err()
                .into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum AccountMeError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Auth
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AuthApi<'a> {
    client: &'a mut KamuApiServerClient,
}

impl AuthApi<'_> {
    pub fn logged_account_id(&self) -> odf::AccountID {
        self.client
            .state
            .logged_in_user
            .as_ref()
            .map(|x| &x.account_id)
            .cloned()
            .expect("Not logged in")
    }

    pub fn set_logged_as(&mut self, token: AccessToken, account_id: odf::AccountID) {
        self.client.state.logged_in_user = Some(LoggedInUser { account_id, token });
    }

    pub fn logout(&mut self) {
        self.client.state.logged_in_user = None;
    }

    pub async fn login_as_kamu(&mut self) -> AccessToken {
        self.login_with_password("kamu", "kamu").await
    }

    pub async fn login_as_e2e_user(&mut self) -> AccessToken {
        // We are using DummyOAuthGithub, so the loginCredentialsJson can be arbitrary
        self.login(indoc::indoc!(
            r#"
            mutation {
              auth {
                login(loginMethod: "oauth_github", loginCredentialsJson: "") {
                  accessToken
                  account {
                    id
                  }
                }
              }
            }
            "#,
        ))
        .await
    }

    pub async fn login_as_e2e_user_with_device_code(&mut self) -> AccessToken {
        // We are using DummyOAuthGithub, so the loginCredentialsJson can be arbitrary
        self.login(
            indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: "oauth_github", loginCredentialsJson: "", deviceCode: "<device_code>") {
                      accessToken
                      account {
                        id
                      }
                    }
                  }
                }
                "#,
            )
            .replace("<device_code>", &PREDEFINED_DEVICE_CODE_UUID.to_string())
            .as_str(),
        )
        .await
    }

    pub async fn login_with_password(&mut self, user: &str, password: &str) -> AccessToken {
        self.login(
            indoc::indoc!(
                r#"
                mutation {
                  auth {
                    login(loginMethod: "password", loginCredentialsJson: "{\"login\":\"<user>\",\"password\":\"<password>\"}") {
                      accessToken
                      account {
                        id
                      }
                    }
                  }
                }
                "#,
            )
            .replace("<user>", user)
            .replace("<password>", password)
            .as_str()
        ).await
    }

    pub async fn login_via_rest(
        &mut self,
        login_method: impl ToString,
        login_credentials_json: serde_json::Value,
    ) -> Result<(), LoginError> {
        let request_body = LoginRequestBody {
            login_method: login_method.to_string(),
            login_credentials_json: serde_json::to_string(&login_credentials_json).int_err()?,
        };
        let request_body_json = serde_json::to_value(request_body).int_err()?;
        let response = self
            .client
            .rest_api_call(
                Method::POST,
                "/platform/login",
                Some(RequestBody::Json(request_body_json)),
            )
            .await;

        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED => Err(LoginError::Unauthorized),
            unexpected_status => Err(format!("Unexpected status: {unexpected_status}")
                .int_err()
                .into()),
        }
    }

    pub async fn token_validate(&self) -> Result<(), TokenValidateError> {
        let response = self
            .client
            .rest_api_call(Method::GET, "/platform/token/validate", None)
            .await;

        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED => Err(TokenValidateError::Unauthorized),
            unexpected_status => Err(format!("Unexpected status: {unexpected_status}")
                .int_err()
                .into()),
        }
    }

    async fn login(&mut self, login_request: &str) -> AccessToken {
        let login_response = self
            .client
            .graphql_api_call(login_request, None)
            .await
            .data();
        let login_node = &login_response["auth"]["login"];

        let access_token = login_node["accessToken"]
            .as_str()
            .map(ToOwned::to_owned)
            .unwrap();
        let account_id = login_node["account"]["id"]
            .as_str()
            .map(|s| odf::AccountID::from_did_str(s).unwrap())
            .unwrap();

        self.client.state.logged_in_user = Some(LoggedInUser {
            account_id,
            token: access_token.clone(),
        });

        access_token
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum LoginError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum TokenValidateError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Dataset
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetApi<'a> {
    client: &'a KamuApiServerClient,
}

impl DatasetApi<'_> {
    pub fn collaboration(&self) -> DatasetCollaborationApi {
        DatasetCollaborationApi {
            client: self.client,
        }
    }

    /// Used for Simple Transfer Protocol
    pub fn get_endpoint(&self, dataset_alias: &odf::DatasetAlias) -> Url {
        let node_url = self.client.get_base_url();
        let url = node_url.join(format!("{dataset_alias}").as_str()).unwrap();
        pretty_assertions::assert_eq!("http", url.scheme());
        url
    }

    /// Used for Smart Transfer Protocol
    pub fn get_odf_endpoint(&self, dataset_alias: &odf::DatasetAlias) -> Url {
        let node_url = self.client.get_odf_node_url();
        let url = node_url.join(format!("{dataset_alias}").as_str()).unwrap();
        pretty_assertions::assert_eq!("odf+http", url.scheme());
        url
    }

    pub async fn by_id(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<DatasetInfoResponse, DatasetByIdError> {
        let response = self
            .client
            .rest_api_call(Method::GET, &format!("datasets/{dataset_id}"), None)
            .await;

        match response.status() {
            StatusCode::OK => Ok(response.json().await.int_err()?),
            StatusCode::NOT_FOUND => Err(DatasetByIdError::NotFound),
            unexpected_status => Err(format!("Unexpected status: {unexpected_status}")
                .int_err()
                .into()),
        }
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
                None,
            )
            .await
            .data();

        let create_response_node = &create_response["datasets"]["createEmpty"];

        pretty_assertions::assert_eq!(Some("Success"), create_response_node["message"].as_str());

        let dataset_id_as_str = create_response_node["dataset"]["id"].as_str().unwrap();
        let alias_str = create_response_node["dataset"]["alias"].as_str().unwrap();
        let dataset_alias = alias_str.parse().unwrap();

        CreateDatasetResponse {
            dataset_id: odf::DatasetID::from_did_str(dataset_id_as_str).unwrap(),
            dataset_alias,
        }
    }

    pub async fn create_dataset(&self, dataset_snapshot_yaml: &str) -> CreateDatasetResponse {
        self.create_dataset_with_visibility(dataset_snapshot_yaml, odf::DatasetVisibility::Public)
            .await
    }

    pub async fn create_dataset_from_snapshot_with_visibility(
        &self,
        snapshot: odf::DatasetSnapshot,
        visibility: odf::DatasetVisibility,
    ) -> CreateDatasetResponse {
        let serialized_snapshot = odf::serde::yaml::YamlDatasetSnapshotSerializer
            .write_manifest_str(&snapshot)
            .unwrap()
            .escape_default()
            .to_string();

        self.create_dataset_with_visibility(&serialized_snapshot, visibility)
            .await
    }

    pub async fn create_dataset_with_visibility(
        &self,
        dataset_snapshot_yaml: &str,
        visibility: odf::DatasetVisibility,
    ) -> CreateDatasetResponse {
        let dataset_visibility_value = if visibility.is_public() {
            "PUBLIC"
        } else {
            "PRIVATE"
        };

        let create_response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    mutation {
                      datasets {
                        createFromSnapshot(snapshot: "<snapshot>", snapshotFormat: YAML, datasetVisibility: "<dataset_visibility_value>") {
                          message
                          ... on CreateDatasetResultSuccess {
                            dataset {
                              id
                              alias
                            }
                          }
                        }
                      }
                    }
                    "#,
                )
                .replace("<snapshot>", dataset_snapshot_yaml)
                .replace("<dataset_visibility_value>", dataset_visibility_value)
                .as_str(),
                None,
            )
            .await
            .data();

        let create_response_node = &create_response["datasets"]["createFromSnapshot"];

        pretty_assertions::assert_eq!(Some("Success"), create_response_node["message"].as_str());

        let dataset_id_as_str = create_response_node["dataset"]["id"].as_str().unwrap();
        let alias_str = create_response_node["dataset"]["alias"].as_str().unwrap();
        let dataset_alias = alias_str.parse().unwrap();

        CreateDatasetResponse {
            dataset_id: odf::DatasetID::from_did_str(dataset_id_as_str).unwrap(),
            dataset_alias,
        }
    }

    pub async fn create_player_scores_dataset(&self) -> CreateDatasetResponse {
        self.create_dataset(&DATASET_ROOT_PLAYER_SCORES_SNAPSHOT)
            .await
    }

    pub async fn create_player_scores_dataset_with_data(&self) -> CreateDatasetResponse {
        let create_response = self.create_player_scores_dataset().await;

        self.ingest_data(
            &create_response.dataset_alias,
            RequestBody::NdJson(DATASET_ROOT_PLAYER_SCORES_INGEST_DATA_NDJSON_CHUNK_1.into()),
        )
        .await;

        create_response
    }

    pub async fn create_leaderboard(&self) -> CreateDatasetResponse {
        self.create_dataset(&DATASET_DERIVATIVE_LEADERBOARD_SNAPSHOT)
            .await
    }

    pub async fn get_role(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Option<AccountToDatasetRelation>, GetDatasetRoleError> {
        let response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      datasets {
                        byId(
                          datasetId: "<dataset_id>"
                        ) {
                          role
                        }
                      }
                    }
                    "#,
                )
                .replace("<dataset_id>", &dataset_id.as_did_str().to_stack_string())
                .as_str(),
                None,
            )
            .await;

        match response {
            Ok(data) => {
                let dataset = &data["datasets"]["byId"];

                if dataset.is_null() {
                    return Err(GetDatasetRoleError::DatasetNotFound);
                }

                let role_node = &dataset["role"];

                if role_node.is_null() {
                    return Ok(None);
                }

                match role_node.as_str().unwrap() {
                    "READER" => Ok(Some(AccountToDatasetRelation::Reader)),
                    "EDITOR" => Ok(Some(AccountToDatasetRelation::Editor)),
                    "MAINTAINER" => Ok(Some(AccountToDatasetRelation::Maintainer)),
                    unexpected_role => Err(format!("Unexpected role: {unexpected_role}")
                        .int_err()
                        .into()),
                }
            }
            Err(errors) => {
                let first_error = errors.first().unwrap();
                let unexpected_message = first_error.message.as_str();

                Err(format!("Unexpected error message: {unexpected_message}")
                    .int_err()
                    .into())
            }
        }
    }

    pub async fn get_visibility(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<odf::DatasetVisibility, GetDatasetVisibilityError> {
        let response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      datasets {
                        byId(
                          datasetId: "<dataset_id>"
                        ) {
                          visibility {
                            __typename
                          }
                        }
                      }
                    }
                    "#,
                )
                .replace("<dataset_id>", &dataset_id.as_did_str().to_stack_string())
                .as_str(),
                None,
            )
            .await;

        match response {
            Ok(data) => {
                let dataset = &data["datasets"]["byId"];

                if dataset.is_null() {
                    Err(GetDatasetVisibilityError::DatasetNotFound)
                } else {
                    let typename = dataset["visibility"]["__typename"].as_str().unwrap();

                    match typename {
                        "PublicDatasetVisibility" => Ok(odf::DatasetVisibility::Public),
                        "PrivateDatasetVisibility" => Ok(odf::DatasetVisibility::Private),
                        unexpected_typename => {
                            Err(format!("Unexpected typename: {unexpected_typename}")
                                .int_err()
                                .into())
                        }
                    }
                }
            }
            Err(errors) => {
                let first_error = errors.first().unwrap();
                let unexpected_message = first_error.message.as_str();

                Err(format!("Unexpected error message: {unexpected_message}")
                    .int_err()
                    .into())
            }
        }
    }

    pub async fn set_visibility(
        &self,
        dataset_id: &odf::DatasetID,
        dataset_visibility: odf::DatasetVisibility,
    ) -> Result<(), SetDatasetVisibilityError> {
        let visibility = match dataset_visibility {
            odf::DatasetVisibility::Private => "private: {}",
            odf::DatasetVisibility::Public => "public: { anonymousAvailable: false }",
        };
        let response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    mutation {
                      datasets {
                        byId(
                          datasetId: "<dataset_id>"
                        ) {
                          setVisibility(visibility: { <dataset_visibility> }) {
                            message
                          }
                        }
                      }
                    }
                    "#,
                )
                .replace("<dataset_id>", &dataset_id.as_did_str().to_stack_string())
                .replace("<dataset_visibility>", visibility)
                .as_str(),
                None,
            )
            .await;

        match response {
            Ok(response) => {
                if response["datasets"]["byId"].is_null() {
                    Err(SetDatasetVisibilityError::DatasetNotFound)
                } else {
                    Ok(())
                }
            }
            Err(errors) => {
                let first_error = errors.first().unwrap();
                let unexpected_message = first_error.message.as_str();

                match unexpected_message {
                    "Dataset access error" => Err(SetDatasetVisibilityError::Access),
                    unexpected_message => {
                        Err(format!("Unexpected error message: {unexpected_message}")
                            .int_err()
                            .into())
                    }
                }
            }
        }
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
                None,
            )
            .await
            .data();

        tail_response["datasets"]["byId"]["data"]["tail"]["data"]["content"]
            .as_str()
            .map(ToOwned::to_owned)
            .unwrap()
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
                None,
            )
            .await
            .data();

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
                    "AddData" => odf::metadata::MetadataEventTypeFlags::ADD_DATA,
                    "ExecuteTransform" => odf::metadata::MetadataEventTypeFlags::EXECUTE_TRANSFORM,
                    "Seed" => odf::metadata::MetadataEventTypeFlags::SEED,
                    "SetPollingSource" => odf::metadata::MetadataEventTypeFlags::SET_POLLING_SOURCE,
                    "SetTransform" => odf::metadata::MetadataEventTypeFlags::SET_TRANSFORM,
                    "SetVocab" => odf::metadata::MetadataEventTypeFlags::SET_VOCAB,
                    "SetAttachments" => odf::metadata::MetadataEventTypeFlags::SET_ATTACHMENTS,
                    "SetInfo" => odf::metadata::MetadataEventTypeFlags::SET_INFO,
                    "SetLicense" => odf::metadata::MetadataEventTypeFlags::SET_LICENSE,
                    "SetDataSchema" => odf::metadata::MetadataEventTypeFlags::SET_DATA_SCHEMA,
                    "AddPushSource" => odf::metadata::MetadataEventTypeFlags::ADD_PUSH_SOURCE,
                    "DisablePushSource" => {
                        odf::metadata::MetadataEventTypeFlags::DISABLE_PUSH_SOURCE
                    }
                    "DisablePollingSource" => {
                        odf::metadata::MetadataEventTypeFlags::DISABLE_POLLING_SOURCE
                    }
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

    pub async fn by_account_name(&self, account_name: &odf::AccountName) -> Vec<AccountDataset> {
        let mut response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      datasets {
                        byAccountName(accountName: "<account_name>") {
                          nodes {
                            id
                            alias
                          }
                        }
                      }
                    }
                    "#
                )
                .replace("<account_name>", account_name)
                .as_str(),
                None,
            )
            .await
            .data();

        let mut result = response["datasets"]["byAccountName"]["nodes"]
            .as_array_mut()
            .unwrap()
            .iter_mut()
            .map(|node| serde_json::from_value::<AccountDataset>(node.take()).unwrap())
            .collect::<Vec<_>>();

        result.sort_by(|left, right| left.alias.cmp(&right.alias));

        result
    }

    pub async fn dependencies(&self, dataset_id: &odf::DatasetID) -> DatasetDependencies {
        let mut response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      datasets {
                        byId(
                          datasetId: "<dataset_id>"
                        ) {
                          metadata {
                            currentUpstreamDependencies {
                              __typename
                              ... on DependencyDatasetResultAccessible {
                                dataset {
                                  id
                                  alias
                                }
                              }
                              ... on DependencyDatasetResultNotAccessible {
                                id
                                message
                              }
                            }
                            currentDownstreamDependencies {
                              __typename
                              ... on DependencyDatasetResultAccessible {
                                dataset {
                                  id
                                  alias
                                }
                              }
                              ... on DependencyDatasetResultNotAccessible {
                                id
                                message
                              }
                            }
                          }
                        }
                      }
                    }
                    "#
                )
                .replace(
                    "<dataset_id>",
                    dataset_id.as_did_str().to_stack_string().as_str(),
                )
                .as_str(),
                None,
            )
            .await
            .data();

        let metadata = &mut response["datasets"]["byId"]["metadata"];

        fn map_dependency(dependency: &mut serde_json::Value) -> DatasetDependency {
            let typename = dependency["__typename"].as_str().unwrap();

            match typename {
                "DependencyDatasetResultAccessible" => {
                    let dataset = dependency["dataset"].take();

                    DatasetDependency::Resolved(
                        serde_json::from_value::<ResolvedDatasetDependency>(dataset).unwrap(),
                    )
                }
                "DependencyDatasetResultNotAccessible" => DatasetDependency::Unresolved(
                    serde_json::from_value::<UnresolvedDatasetDependency>(dependency.take())
                        .unwrap(),
                ),
                unexpected_typename => {
                    unreachable!("Unexpected typename: {unexpected_typename}");
                }
            }
        }

        let mut upstream = metadata["currentUpstreamDependencies"]
            .as_array_mut()
            .map(|dependencies| {
                dependencies
                    .iter_mut()
                    .map(map_dependency)
                    .collect::<Vec<_>>()
            })
            .unwrap();
        let mut downstream = metadata["currentDownstreamDependencies"]
            .as_array_mut()
            .map(|dependencies| {
                dependencies
                    .iter_mut()
                    .map(map_dependency)
                    .collect::<Vec<_>>()
            })
            .unwrap();

        use std::cmp::Ordering;

        fn comparator(left: &DatasetDependency, right: &DatasetDependency) -> Ordering {
            match (left, right) {
                (DatasetDependency::Resolved(l), DatasetDependency::Resolved(r)) => {
                    l.alias.cmp(&r.alias)
                }
                (_, DatasetDependency::Resolved(_)) => Ordering::Less,
                (DatasetDependency::Resolved(_), _) => Ordering::Greater,
                _ => Ordering::Equal,
            }
        }

        upstream.sort_by(comparator);
        downstream.sort_by(comparator);

        DatasetDependencies {
            upstream,
            downstream,
        }
    }
}

pub struct DatasetCollaborationApi<'a> {
    client: &'a KamuApiServerClient,
}

impl DatasetCollaborationApi<'_> {
    pub async fn account_roles(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<AccountRolesResponse, DatasetCollaborationAccountRolesError> {
        let response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      datasets {
                        byId(datasetId: "<dataset_id>") {
                          collaboration {
                            accountRoles {
                              nodes {
                                account {
                                  id
                                  accountName
                                }
                                role
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
                None,
            )
            .await;

        match response {
            Ok(data) => {
                let dataset = &data["datasets"]["byId"];

                if dataset.is_null() {
                    return Err(DatasetCollaborationAccountRolesError::DatasetNotFound);
                }

                let nodes = dataset["collaboration"]["accountRoles"]["nodes"]
                    .as_array()
                    .unwrap();
                let mut accounts_with_roles = nodes
                    .iter()
                    .map(|node| {
                        let account_node = &node["account"];
                        let account_id = account_node["id"].as_str().unwrap();
                        let account_name = account_node["accountName"].as_str().unwrap();
                        let role = node["role"].as_str().unwrap();

                        AccountWithRole {
                            account_id: odf::AccountID::from_did_str(account_id).unwrap(),
                            account_name: odf::AccountName::from_str(account_name).unwrap(),
                            role: role.to_lowercase().parse().unwrap(),
                        }
                    })
                    .collect::<Vec<_>>();

                accounts_with_roles.sort_by(|a, b| a.account_name.cmp(&b.account_name));

                Ok(AccountRolesResponse {
                    accounts_with_roles,
                })
            }
            Err(errors) => {
                let first_error = errors.first().unwrap();
                let unexpected_message = first_error.message.as_str();

                match unexpected_message {
                    "Dataset access error" => Err(DatasetCollaborationAccountRolesError::Access),
                    unexpected_message => {
                        Err(format!("Unexpected error message: {unexpected_message}")
                            .int_err()
                            .into())
                    }
                }
            }
        }
    }

    pub async fn set_role(
        &self,
        dataset_id: &odf::DatasetID,
        account_id: &odf::AccountID,
        role: AccountToDatasetRelation,
    ) -> Result<(), DatasetCollaborationSetRoleError> {
        let role = match role {
            AccountToDatasetRelation::Reader => "READER",
            AccountToDatasetRelation::Editor => "EDITOR",
            AccountToDatasetRelation::Maintainer => "MAINTAINER",
        };
        let response = self
            .client
            .graphql_api_call_ex(
                async_graphql::Request::new(indoc::indoc!(
                    r#"
                    mutation ($dataset_id: DatasetID!, $account_id: AccountID!, $role: DatasetAccessRole!) {
                      datasets {
                        byId(datasetId: $dataset_id) {
                          collaboration {
                            setRole(accountId: $account_id, role: $role) {
                              __typename
                            }
                          }
                        }
                      }
                    }
                    "#,
                ))
                .variables(async_graphql::Variables::from_value(
                    async_graphql::value!({
                        "dataset_id": dataset_id.as_did_str().to_stack_string().as_str(),
                        "account_id": account_id.to_string(),
                        "role": role,
                    }),
                )),
            )
            .await;

        if response.is_ok() {
            let dataset = &response.into_json_data()["datasets"]["byId"];

            if dataset.is_null() {
                return Err(DatasetCollaborationSetRoleError::DatasetNotFound);
            }

            Ok(())
        } else {
            Err(
                format!("Unexpected error message: {:?}", response.error_messages())
                    .int_err()
                    .into(),
            )
        }
    }

    pub async fn unset_role(
        &self,
        dataset_id: &odf::DatasetID,
        account_ids: &[&odf::AccountID],
    ) -> Result<(), DatasetCollaborationUnsetRoleError> {
        let response = self
            .client
            .graphql_api_call(
                &indoc::indoc!(
                    r#"
                    mutation {
                      datasets {
                        byId(datasetId: "<dataset_id>") {
                          collaboration {
                            unsetRoles(accountIds: [<account_ids>]) {
                              __typename
                            }
                          }
                        }
                      }
                    }
                    "#,
                )
                .replace("<dataset_id>", &dataset_id.as_did_str().to_stack_string())
                .replace(
                    "<account_ids>",
                    &account_ids
                        .iter()
                        .map(|id| format!("\"{id}\""))
                        .intersperse(",".to_string())
                        .collect::<String>(),
                ),
                None,
            )
            .await;

        match response {
            Ok(data) => {
                let dataset = &data["datasets"]["byId"];

                if dataset.is_null() {
                    return Err(DatasetCollaborationUnsetRoleError::DatasetNotFound);
                }

                Ok(())
            }
            Err(errors) => {
                let first_error = errors.first().unwrap();
                let unexpected_message = first_error.message.as_str();

                Err(format!("Unexpected error message: {unexpected_message}")
                    .int_err()
                    .into())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct CreateDatasetResponse {
    pub dataset_id: odf::DatasetID,
    pub dataset_alias: odf::DatasetAlias,
}

#[derive(Debug)]
pub struct DatasetBlock {
    pub block_hash: odf::Multihash,
    pub prev_block_hash: Option<odf::Multihash>,
    pub system_time: DateTime<Utc>,
    pub sequence_number: u64,
    pub event: odf::metadata::MetadataEventTypeFlags,
}

#[derive(Debug)]
pub struct DatasetBlocksResponse {
    pub blocks: Vec<DatasetBlock>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct AccountDataset {
    pub id: odf::DatasetID,
    pub alias: odf::DatasetAlias,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct ResolvedDatasetDependency {
    pub id: odf::DatasetID,
    pub alias: odf::DatasetAlias,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct UnresolvedDatasetDependency {
    pub id: odf::DatasetID,
    pub message: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DatasetDependency {
    Resolved(ResolvedDatasetDependency),
    Unresolved(UnresolvedDatasetDependency),
}

#[derive(Debug, PartialEq, Eq)]
pub struct DatasetDependencies {
    pub upstream: Vec<DatasetDependency>,
    pub downstream: Vec<DatasetDependency>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountWithRole {
    pub account_id: odf::AccountID,
    pub account_name: odf::AccountName,
    pub role: AccountToDatasetRelation,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRolesResponse {
    pub accounts_with_roles: Vec<AccountWithRole>,
}

impl AccountRolesResponse {
    pub fn new(accounts_with_roles: Vec<AccountWithRole>) -> Self {
        Self {
            accounts_with_roles,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetDatasetRoleError {
    #[error("Dataset not found")]
    DatasetNotFound,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for GetDatasetRoleError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DatasetNotFound, Self::DatasetNotFound) => true,
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

impl Eq for GetDatasetRoleError {}

#[derive(Error, Debug)]
pub enum GetDatasetVisibilityError {
    #[error("Dataset not found")]
    DatasetNotFound,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for GetDatasetVisibilityError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DatasetNotFound, Self::DatasetNotFound) => true,
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

impl Eq for GetDatasetVisibilityError {}

#[derive(Error, Debug)]
pub enum SetDatasetVisibilityError {
    #[error("Dataset not found")]
    DatasetNotFound,
    #[error("Access")]
    Access,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for SetDatasetVisibilityError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DatasetNotFound, Self::DatasetNotFound) | (Self::Access, Self::Access) => true,
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

impl Eq for SetDatasetVisibilityError {}

#[derive(Error, Debug)]
pub enum DatasetByIdError {
    #[error("Not found")]
    NotFound,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum DatasetCollaborationAccountRolesError {
    #[error("Dataset not found")]
    DatasetNotFound,
    #[error("Access")]
    Access,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for DatasetCollaborationAccountRolesError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Access, Self::Access) | (Self::DatasetNotFound, Self::DatasetNotFound) => true,
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

impl Eq for DatasetCollaborationAccountRolesError {}

#[derive(Error, Debug)]
pub enum DatasetCollaborationSetRoleError {
    #[error("Dataset not found")]
    DatasetNotFound,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for DatasetCollaborationSetRoleError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DatasetNotFound, Self::DatasetNotFound) => true,
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

impl Eq for DatasetCollaborationSetRoleError {}

#[derive(Error, Debug)]
pub enum DatasetCollaborationUnsetRoleError {
    #[error("Dataset not found")]
    DatasetNotFound,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for DatasetCollaborationUnsetRoleError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::DatasetNotFound, Self::DatasetNotFound) => true,
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

impl Eq for DatasetCollaborationUnsetRoleError {}

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
                    &format!("{dataset_flow_type:?}").to_case(Case::UpperSnake),
                )
                .as_str(),
                None,
            )
            .await
            .data();

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

    // Method to wait for a flow to finish
    // Args:
    // - dataset_id: The ID of the dataset to check
    // - expected_flow_count: The exact number of flows to wait for. Useful for
    //   testcases
    // where we have 2 or more flows for specific dataset which are triggered
    // asynchronously For most cases will be used 1
    pub async fn wait(&self, dataset_id: &odf::DatasetID, expected_flow_count: usize) {
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
                    None,
                )
                .await
                .data();

            let edges = response["datasets"]["byId"]["flows"]["runs"]["listFlows"]["edges"]
                .as_array()
                .unwrap();
            if edges.len() < expected_flow_count {
                return Err(());
            }
            let all_finished = edges.iter().all(|edge| {
                let status = edge["node"]["status"].as_str().unwrap();

                status == "FINISHED"
            });

            if all_finished { Ok(()) } else { Err(()) }
        })
        .await
        .unwrap();
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub enum FlowTriggerResponse {
    Success(FlowID),
    Error(String),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: ODF, core
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OdfCoreApi<'a> {
    client: &'a KamuApiServerClient,
}

impl OdfCoreApi<'_> {
    pub async fn info(&self) -> Result<NodeInfoResponse, InternalError> {
        let response = self.client.rest_api_call(Method::GET, "info", None).await;

        match response.status() {
            StatusCode::OK => response.json().await.int_err(),
            unexpected_status => panic!("Unexpected status: {unexpected_status}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: ODF, transfer
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OdfTransferApi<'a> {
    client: &'a KamuApiServerClient,
}

impl OdfTransferApi<'_> {
    pub async fn metadata_block_hash_by_ref(
        &self,
        dataset_alias: &odf::DatasetAlias,
        block_ref: odf::BlockRef,
    ) -> Result<odf::Multihash, MetadataBlockHashByRefError> {
        let response = self
            .client
            .rest_api_call(
                Method::GET,
                &format!("{dataset_alias}/refs/{block_ref}"),
                None,
            )
            .await;

        match response.status() {
            StatusCode::OK => {
                let raw_response_body = response.text().await.int_err()?;
                Ok(odf::Multihash::from_multibase(&raw_response_body).int_err()?)
            }
            StatusCode::NOT_FOUND => Err(MetadataBlockHashByRefError::NotFound),
            unexpected_status => Err(format!("Unexpected status: {unexpected_status}")
                .int_err()
                .into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum MetadataBlockHashByRefError {
    #[error("Not found")]
    NotFound,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: ODF, query
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OdfQuery<'a> {
    client: &'a KamuApiServerClient,
}

impl OdfQuery<'_> {
    pub async fn query(&self, query: &str) -> Result<String, QueryError> {
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
                None,
            )
            .await
            .data();
        let query_node = &response["data"]["query"];

        match query_node["__typename"].as_str() {
            Some("DataQueryResultSuccess") => {
                let content = query_node["data"]["content"]
                    .as_str()
                    .map(ToOwned::to_owned)
                    .unwrap();

                Ok(content)
            }
            Some("DataQueryResultError") => {
                let kind = query_node["errorKind"]
                    .as_str()
                    .map(ToOwned::to_owned)
                    .unwrap();
                let message = query_node["errorMessage"]
                    .as_str()
                    .map(ToOwned::to_owned)
                    .unwrap();

                Err(QueryExecutionError {
                    error_kind: kind,
                    error_message: message,
                }
                .into())
            }
            unexpected_type_name => Err(format!("Unexpected __typename: {unexpected_type_name:?}")
                .int_err()
                .into()),
        }
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
        .unwrap()
    }

    pub async fn query_via_rest(
        &self,
        options: &QueryRequest,
    ) -> Result<QueryResponse, InternalError> {
        let request_body_json = serde_json::to_value(options).int_err()?;

        let response = self
            .client
            .rest_api_call(
                Method::POST,
                "/query",
                Some(RequestBody::Json(request_body_json)),
            )
            .await;

        match response.status() {
            StatusCode::OK => Ok(response.json().await.int_err()?),
            unexpected_status => {
                let message = response.text().await.int_err()?;

                Err(format!("Unexpected status: {unexpected_status}, message: {message}").int_err())
            }
        }
    }

    pub async fn verify(&self, options: VerifyRequest) -> Result<VerifyResponse, InternalError> {
        let request_body_json = serde_json::to_value(&options).int_err()?;

        let response = self
            .client
            .rest_api_call(
                Method::POST,
                "/verify",
                Some(RequestBody::Json(request_body_json)),
            )
            .await;

        match response.status() {
            StatusCode::OK => Ok(response.json().await.int_err()?),
            unexpected_status => {
                let message = response.text().await.int_err()?;

                Err(format!("Unexpected status: {unexpected_status}, message: {message}").int_err())
            }
        }
    }

    pub async fn metadata(
        &self,
        dataset_alias: &odf::DatasetAlias,
        maybe_include_events: Option<CommaSeparatedSet<MetadataInclude>>,
    ) -> Result<DatasetMetadataResponse, InternalError> {
        let include_events =
            maybe_include_events.unwrap_or_else(DatasetMetadataParams::default_include);
        let include_events_json = serde_json::to_value(include_events).int_err()?;
        let include_query_param_value = include_events_json
            .as_str()
            .ok_or_else(|| "Failed get JSON as string".int_err())?
            .trim_matches('"');

        let response = self
            .client
            .rest_api_call(
                Method::GET,
                &format!("{dataset_alias}/metadata?include={include_query_param_value}"),
                None,
            )
            .await;

        let status = response.status();

        if status != StatusCode::OK {
            return Err(format!("Unexpected status: {status}").int_err());
        }

        response.json::<DatasetMetadataResponse>().await.int_err()
    }

    pub async fn tail(&self, dataset_alias: &odf::DatasetAlias) -> serde_json::Value {
        let response = self
            .client
            .rest_api_call(Method::GET, &format!("{dataset_alias}/tail"), None)
            .await;

        match response.status() {
            StatusCode::OK => response.json().await.unwrap(),
            StatusCode::NO_CONTENT => serde_json::Value::Null,
            unexpected_status => panic!("Unexpected status: {unexpected_status}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug, PartialEq)]
#[error("{self:?}")]
pub struct QueryExecutionError {
    pub error_kind: String,
    pub error_message: String,
}

#[derive(Error, Debug)]
pub enum QueryError {
    #[error(transparent)]
    ExecutionError(#[from] QueryExecutionError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for QueryError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::ExecutionError(a), Self::ExecutionError(b)) => a.eq(b),
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

impl Eq for QueryError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Search
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SearchApi<'a> {
    client: &'a KamuApiServerClient,
}

impl SearchApi<'_> {
    pub async fn search(&self, query: &str) -> Vec<FoundDatasetItem> {
        let mut response = self
            .client
            .graphql_api_call(
                indoc::indoc!(
                    r#"
                    query {
                      search {
                        query(query: "<query>") {
                          nodes {
                            __typename
                            ... on Dataset {
                              id
                              alias
                            }
                          }
                        }
                      }
                    }
                    "#
                )
                .replace("<query>", query)
                .as_str(),
                None,
            )
            .await
            .data();

        let mut result = response["search"]["query"]["nodes"]
            .as_array_mut()
            .map(|nodes| {
                nodes
                    .iter_mut()
                    .map(|node| {
                        let typename = node["__typename"].as_str().unwrap();

                        pretty_assertions::assert_eq!("Dataset", typename);

                        serde_json::from_value::<FoundDatasetItem>(node.take()).unwrap()
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap();

        result.sort_by(|left, right| left.alias.cmp(&right.alias));

        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct FoundDatasetItem {
    pub id: odf::DatasetID,
    pub alias: odf::DatasetAlias,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Swagger
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct SwaggerApi<'a> {
    client: &'a KamuApiServerClient,
}

impl SwaggerApi<'_> {
    pub async fn main_page(&self) -> String {
        let response = self
            .client
            .rest_api_call(Method::GET, "/openapi", None)
            .await;

        pretty_assertions::assert_eq!(StatusCode::OK, response.status());

        response.text().await.unwrap()
    }

    pub async fn schema(&self) -> serde_json::Value {
        let response = self
            .client
            .rest_api_call(Method::GET, "/openapi.json", None)
            .await;

        pretty_assertions::assert_eq!(StatusCode::OK, response.status());

        response.json().await.unwrap()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: Upload
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct UploadApi<'a> {
    client: &'a KamuApiServerClient,
}

impl UploadApi<'_> {
    pub async fn prepare(
        &self,
        options: PlatformFileUploadQuery,
    ) -> Result<UploadContext, UploadPrepareError> {
        let query_params = serde_urlencoded::to_string(options).int_err()?;

        let response = self
            .client
            .rest_api_call(
                Method::POST,
                &format!("/platform/file/upload/prepare?{query_params}"),
                None,
            )
            .await;

        match response.status() {
            StatusCode::OK => Ok(response.json().await.int_err()?),
            StatusCode::UNAUTHORIZED => Err(UploadPrepareError::Unauthorized),
            unexpected_status => Err(format!("Unexpected status: {unexpected_status}")
                .int_err()
                .into()),
        }
    }

    pub async fn upload_file(
        &self,
        upload_context: &UploadContext,
        file_name: &str,
        file_data: &str,
    ) -> Result<(), UploadFileError> {
        pretty_assertions::assert_eq!(true, upload_context.use_multipart);

        use reqwest::multipart::{Form, Part};

        let headers = convert_headers(&upload_context.headers);
        let form = Form::new().part(
            "file",
            Part::text(file_data.to_string())
                .file_name(file_name.to_string())
                .mime_str("text/plain")
                .int_err()?,
        );

        let response = reqwest::Client::new()
            .post(upload_context.upload_url.clone())
            .headers(headers)
            .multipart(form)
            .send()
            .await
            .int_err()?;

        match response.status() {
            StatusCode::OK => Ok(()),
            StatusCode::UNAUTHORIZED => Err(UploadFileError::Unauthorized),
            unexpected_status => Err(format!("Unexpected status: {unexpected_status}")
                .int_err()
                .into()),
        }
    }

    pub async fn get_file_content(
        &self,
        upload_context: &UploadContext,
    ) -> Result<String, UploadFileError> {
        let headers = convert_headers(&upload_context.headers);
        let response = reqwest::Client::new()
            .get(upload_context.upload_url.clone())
            .headers(headers)
            .send()
            .await
            .int_err()?;

        match response.status() {
            StatusCode::OK => Ok(response.text().await.int_err()?),
            StatusCode::UNAUTHORIZED => Err(UploadFileError::Unauthorized),
            unexpected_status => Err(format!("Unexpected status: {unexpected_status}")
                .int_err()
                .into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum UploadPrepareError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
pub enum UploadFileError {
    #[error("Unauthorized")]
    Unauthorized,
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Helpers
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn convert_headers(headers: &[(String, String)]) -> reqwest::header::HeaderMap {
    use reqwest::header::{HeaderMap, HeaderName, HeaderValue};

    headers
        .iter()
        .fold(HeaderMap::new(), |mut acc, (header, value)| {
            acc.insert(
                HeaderName::from_str(header).unwrap(),
                HeaderValue::from_str(value).unwrap(),
            );
            acc
        })
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
