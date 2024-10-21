// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use opendatafabric::DatasetAlias;
use reqwest::{Method, Response, StatusCode, Url};
use serde::Deserialize;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;

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
pub struct KamuApiServerClient {
    http_client: reqwest::Client,
    server_base_url: Url,
}

impl KamuApiServerClient {
    pub fn new(server_base_url: Url) -> Self {
        let http_client = reqwest::Client::new();

        Self {
            http_client,
            server_base_url,
        }
    }

    pub async fn ready(&self) -> Result<(), InternalError> {
        let retry_strategy = FixedInterval::from_millis(1_000).take(10);
        let response = Retry::spawn(retry_strategy, || async {
            let endpoint = self.server_base_url.join("e2e/health").unwrap();
            let response = self.http_client.get(endpoint).send().await.int_err()?;

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
        let endpoint = self.server_base_url.join("e2e/shutdown").unwrap();
        let response = self.http_client.post(endpoint).send().await.int_err()?;

        let status = response.status();
        if status != StatusCode::OK {
            InternalError::bail(format!("Unexpected shutdown response status: {status}",))?;
        }

        Ok(())
    }

    pub fn get_base_url(&self) -> &Url {
        &self.server_base_url
    }

    pub fn get_node_url(&self) -> Url {
        let mut node_url = Url::parse("odf+http://host").unwrap();
        let base_url = self.get_base_url();

        node_url.set_host(base_url.host_str()).unwrap();
        node_url.set_port(base_url.port()).unwrap();

        node_url
    }

    pub fn get_dataset_endpoint(&self, dataset_alias: &DatasetAlias) -> Url {
        let node_url = self.get_node_url();

        node_url.join(format!("{dataset_alias}").as_str()).unwrap()
    }

    pub async fn rest_api_call(
        &self,
        token: Option<String>,
        method: Method,
        endpoint: &str,
        request_body: Option<RequestBody>,
    ) -> Response {
        let endpoint = self.server_base_url.join(endpoint).unwrap();
        let mut request_builder = match method {
            Method::GET => self.http_client.get(endpoint),
            Method::POST => self.http_client.post(endpoint),
            Method::PUT => self.http_client.put(endpoint),
            _ => {
                unimplemented!()
            }
        };

        if let Some(token) = token {
            request_builder = request_builder.bearer_auth(token);
        }

        if let Some(request_body) = request_body {
            request_builder = match request_body {
                RequestBody::Json(value) => request_builder.json(&value),
                RequestBody::NdJson(value) => request_builder
                    .header("Content-Type", "application/x-ndjson")
                    .body(value),
            }
        };

        match request_builder.send().await {
            Ok(response_body) => response_body,
            Err(e) => panic!("Unexpected send error: {e:?}"),
        }
    }

    pub async fn rest_api_call_assert(
        &self,
        token: Option<String>,
        method: Method,
        endpoint: &str,
        request_body: Option<RequestBody>,
        expected_status: StatusCode,
        expected_response_body: Option<ExpectedResponseBody>,
    ) {
        let response = self
            .rest_api_call(token, method, endpoint, request_body)
            .await;

        pretty_assertions::assert_eq!(expected_status, response.status());

        if let Some(expected_response_body) = expected_response_body {
            self.rest_api_call_response_body_assert(response, expected_response_body)
                .await;
        };
    }

    pub async fn graphql_api_call(
        &self,
        query: &str,
        maybe_token: Option<String>,
    ) -> serde_json::Value {
        let response = self.graphql_api_call_impl(query, maybe_token).await;
        let response_body = response.json::<GraphQLResponseBody>().await.unwrap();

        match response_body.data {
            Some(response_body) => response_body,
            None => panic!("Unexpected response body: {response_body:?}"),
        }
    }

    pub async fn graphql_api_call_assert(
        &self,
        query: &str,
        expected_response: Result<&str, &str>,
    ) {
        self.graphql_api_call_assert_impl(query, expected_response, None)
            .await;
    }

    pub async fn graphql_api_call_assert_with_token(
        &self,
        token: String,
        query: &str,
        expected_response: Result<&str, &str>,
    ) {
        self.graphql_api_call_assert_impl(query, expected_response, Some(token))
            .await;
    }

    pub async fn rest_api_call_response_body_assert(
        &self,
        response: Response,
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
        };
    }

    async fn graphql_api_call_impl(&self, query: &str, maybe_token: Option<String>) -> Response {
        let endpoint = self.server_base_url.join("graphql").unwrap();
        let request_data = serde_json::json!({
           "query": query
        });

        let mut request_builder = self.http_client.post(endpoint).json(&request_data);

        if let Some(token) = maybe_token {
            request_builder = request_builder.bearer_auth(token);
        }

        request_builder.send().await.unwrap()
    }

    async fn graphql_api_call_assert_impl(
        &self,
        query: &str,
        expected_response: Result<&str, &str>,
        maybe_token: Option<String>,
    ) {
        let response = self.graphql_api_call_impl(query, maybe_token).await;

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

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
