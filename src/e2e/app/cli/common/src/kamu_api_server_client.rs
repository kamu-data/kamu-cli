// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use internal_error::{InternalError, ResultIntoInternal};
use reqwest::{Method, StatusCode, Url};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_retry::Retry;
use tokio_retry::strategy::FixedInterval;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub type AccessToken = String;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum RequestBody {
    Json(serde_json::Value),
    NdJson(String),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub enum ExpectedResponseBody {
    Json(serde_json::Value),
    Plain(String),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct LoggedInUser {
    pub account_id: odf::AccountID,
    pub token: AccessToken,
}

#[derive(Clone, Default)]
pub struct KamuApiServerClientState {
    pub logged_in_user: Option<LoggedInUser>,
}

#[derive(Clone)]
pub struct KamuApiServerClient {
    http_client: reqwest::Client,
    server_base_url: Url,
    workspace_path: PathBuf,
    pub(crate) state: KamuApiServerClientState,
}

impl KamuApiServerClient {
    pub fn new(server_base_url: Url, workspace_path: PathBuf) -> Self {
        let http_client = reqwest::Client::new();

        Self {
            http_client,
            workspace_path,
            server_base_url,
            state: KamuApiServerClientState::default(),
        }
    }

    pub fn e2e(&self) -> E2EApi {
        E2EApi { client: self }
    }

    pub fn get_base_url(&self) -> &Url {
        &self.server_base_url
    }

    pub fn get_workspace_path(&self) -> &Path {
        &self.workspace_path
    }

    pub fn get_odf_node_url(&self) -> Url {
        let mut node_url = Url::parse("odf+http://host").unwrap();
        let base_url = self.get_base_url();

        node_url.set_host(base_url.host_str()).unwrap();
        node_url.set_port(base_url.port()).unwrap();

        node_url
    }

    pub async fn rest_api_call(
        &self,
        method: Method,
        endpoint: &str,
        request_body: Option<RequestBody>,
    ) -> reqwest::Response {
        let endpoint = self.server_base_url.join(endpoint).unwrap();
        let mut request_builder = match method {
            Method::GET => self.http_client.get(endpoint),
            Method::POST => self.http_client.post(endpoint),
            Method::PUT => self.http_client.put(endpoint),
            _ => {
                unimplemented!()
            }
        };

        if let Some(logged_in_user) = &self.state.logged_in_user {
            request_builder = request_builder.bearer_auth(&logged_in_user.token);
        }

        if let Some(request_body) = request_body {
            request_builder = match request_body {
                RequestBody::Json(value) => request_builder.json(&value),
                RequestBody::NdJson(value) => request_builder
                    .header("Content-Type", "application/x-ndjson")
                    .body(value),
            }
        }

        match request_builder.send().await {
            Ok(response_body) => response_body,
            Err(e) => panic!("Unexpected send error: {e:?}"),
        }
    }

    pub async fn rest_api_call_assert(
        &self,
        method: Method,
        endpoint: &str,
        request_body: Option<RequestBody>,
        expected_status: StatusCode,
        expected_response_body: Option<ExpectedResponseBody>,
    ) {
        let response = self.rest_api_call(method, endpoint, request_body).await;

        pretty_assertions::assert_eq!(expected_status, response.status());

        if let Some(expected_response_body) = expected_response_body {
            self.rest_api_call_response_body_assert(response, expected_response_body)
                .await;
        }
    }

    /// NOTE: Please use [`Self::graphql_api_call_ex()`] instead of this method
    pub async fn graphql_api_call(
        &self,
        query: &str,
        variables: Option<serde_json::Value>,
    ) -> GraphQLResponse {
        let response = self
            .graphql_api_call_impl(&json!({
               "query": query,
               "variables": variables,
            }))
            .await;

        let response_body = response.json::<async_graphql::Response>().await.unwrap();

        if !response_body.errors.is_empty() {
            Err(response_body.errors)
        } else {
            Ok(response_body.data.into_json().unwrap())
        }
    }

    pub async fn graphql_api_call_ex(
        &self,
        request: async_graphql::Request,
    ) -> async_graphql::Response {
        let response = self.graphql_api_call_impl(&request).await;

        response.json::<async_graphql::Response>().await.unwrap()
    }

    /// NOTE: Please use [`Self::graphql_api_call_ex()`] instead of this method
    /// and explicit [`pretty_assertions::assert_eq!()`]
    pub async fn graphql_api_call_assert(
        &self,
        query: &str,
        expected_response: Result<&str, &str>,
    ) {
        self.graphql_api_call_assert_impl(query, expected_response)
            .await;
    }

    async fn rest_api_call_response_body_assert(
        &self,
        response: reqwest::Response,
        expected_response_body: ExpectedResponseBody,
    ) {
        match expected_response_body {
            ExpectedResponseBody::Json(expected_pretty_json_response_body) => {
                let actual_response_body: serde_json::Value = response.json().await.unwrap();

                pretty_assertions::assert_eq!(
                    expected_pretty_json_response_body,
                    actual_response_body
                );
            }
            ExpectedResponseBody::Plain(expected_plain_response_body) => {
                let actual_response_body =
                    String::from_utf8(response.bytes().await.unwrap().into()).unwrap();

                pretty_assertions::assert_eq!(expected_plain_response_body, actual_response_body);
            }
        }
    }

    async fn graphql_api_call_impl<T: Serialize + ?Sized>(
        &self,
        request_data: &T,
    ) -> reqwest::Response {
        let endpoint = self.server_base_url.join("graphql").unwrap();

        let mut request_builder = self.http_client.post(endpoint).json(&request_data);

        if let Some(logged_in_user) = &self.state.logged_in_user {
            request_builder = request_builder.bearer_auth(&logged_in_user.token);
        }

        request_builder.send().await.unwrap()
    }

    async fn graphql_api_call_assert_impl(
        &self,
        query: &str,
        expected_response: Result<&str, &str>,
    ) {
        let response = self
            .graphql_api_call_impl(&json!({
               "query": query,
            }))
            .await;

        pretty_assertions::assert_eq!(StatusCode::OK, response.status());

        let pretty_response_body: GraphQLPrettyResponseBody =
            match response.json::<GraphQLResponseBody>().await {
                Ok(response_body) => response_body.into(),
                Err(e) => panic!("Unexpected parsing error: {e:?}"),
            };

        match (
            pretty_response_body.errors,
            pretty_response_body.data,
            expected_response,
        ) {
            (Some(actual_error), None, Err(expected_error)) => {
                pretty_assertions::assert_eq!(
                    expected_error,
                    // Let's add \n for the sake of convenience of passing the expected result
                    format!("{actual_error}\n")
                );
            }
            (None, Some(actual_response), Ok(expected_response)) => {
                // Let's add \n for the sake of convenience of passing the expected result
                pretty_assertions::assert_eq!(expected_response, format!("{actual_response}\n"));
            }
            (Some(actual_error), None, Ok(_)) => {
                panic!(
                    "An successful response was expected, but an erroneous one was \
                     received:\n{actual_error}"
                )
            }
            (None, Some(actual_response), Err(_)) => {
                panic!(
                    "An error response was expected, but a successful one was \
                     received:\n{actual_response}"
                )
            }
            _ => unreachable!(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// API: E2E
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct E2EApi<'a> {
    client: &'a KamuApiServerClient,
}

impl E2EApi<'_> {
    pub async fn ready(&self) -> Result<(), InternalError> {
        let retry_strategy = FixedInterval::from_millis(1_000).take(10);
        let response = Retry::spawn(retry_strategy, || async {
            let endpoint = self.client.server_base_url.join("e2e/health").unwrap();
            let response = self
                .client
                .http_client
                .get(endpoint)
                .send()
                .await
                .int_err()?;

            Ok(response)
        })
        .await?;

        let status = response.status();
        if status != StatusCode::OK {
            InternalError::bail(format!("Unexpected health response status: {status}",))?;
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<(), InternalError> {
        let endpoint = self.client.server_base_url.join("e2e/shutdown").unwrap();
        let response = self
            .client
            .http_client
            .post(endpoint)
            .send()
            .await
            .int_err()?;

        let status = response.status();
        if status != StatusCode::OK {
            InternalError::bail(format!("Unexpected shutdown response status: {status}",))?;
        }

        Ok(())
    }

    pub async fn set_system_time(&self, t: DateTime<Utc>) {
        let endpoint = self.client.server_base_url.join("e2e/system_time").unwrap();
        // To avoid making a dependency, we just use a json!() macro
        let request = json!({
            "newSystemTime": t
        });

        let response = self
            .client
            .http_client
            .post(endpoint)
            .json(&request)
            .send()
            .await
            .unwrap();

        let status = response.status();
        if status != StatusCode::OK {
            let response_data = response.text().await.unwrap();

            panic!("Unexpected set system time response status: {status}, {response_data}");
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
struct GraphQLResponseBody {
    data: Option<serde_json::Value>,
    errors: Option<serde_json::Value>,
}

struct GraphQLPrettyResponseBody {
    data: Option<String>,
    errors: Option<String>,
}

impl From<GraphQLResponseBody> for GraphQLPrettyResponseBody {
    fn from(value: GraphQLResponseBody) -> Self {
        Self {
            data: value
                .data
                .map(|v| serde_json::to_string_pretty(&v).unwrap()),
            errors: value
                .errors
                .map(|v| serde_json::to_string_pretty(&v).unwrap()),
        }
    }
}

pub type GraphQLResponse = Result<serde_json::Value, Vec<async_graphql::ServerError>>;

pub trait GraphQLResponseExt {
    fn data(self) -> serde_json::Value;
}

impl GraphQLResponseExt for GraphQLResponse {
    fn data(self) -> serde_json::Value {
        match self {
            Ok(data) => data,
            Err(e) => panic!("Unexpected errors: {e:?}"),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
