// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use reqwest::{Method, Response, StatusCode, Url};
use serde::Deserialize;
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;

////////////////////////////////////////////////////////////////////////////////

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

    pub async fn rest_api_call_assert(
        &self,
        method: Method,
        endpoint: &str,
        request_body: Option<serde_json::Value>,
        expected_status: StatusCode,
        expected_response_body: Option<serde_json::Value>,
    ) {
        let endpoint = self.server_base_url.join(endpoint).unwrap();
        let mut request_builder = match method {
            Method::GET => self.http_client.get(endpoint),
            Method::POST => self.http_client.post(endpoint),
            Method::PUT => self.http_client.put(endpoint),
            _ => {
                unimplemented!()
            }
        };

        if let Some(request_body) = request_body {
            request_builder = request_builder.json(&request_body);
        };

        let response = request_builder.send().await.unwrap();

        assert_eq!(expected_status, response.status());

        if let Some(expected_response_body) = expected_response_body {
            let actual_response_body: serde_json::Value = response.json().await.unwrap();
            let pretty_actual_response_body =
                serde_json::to_string_pretty(&actual_response_body).unwrap();

            assert_eq!(
                expected_response_body,
                // Let's add \n for the sake of convenience of passing the expected result
                format!("{pretty_actual_response_body}\n")
            );
        };
    }

    pub async fn graphql_api_call(
        &self,
        query: &str,
        maybe_token: Option<String>,
    ) -> serde_json::Value {
        let response = self.graphql_api_call_impl(query, maybe_token).await;
        let response_body = response.json::<GraphQLResponseBody>().await.unwrap();

        response_body.data.unwrap()
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

        assert_eq!(StatusCode::OK, response.status());

        let pretty_response_body: GraphQLPrettyResponseBody =
            response.json::<GraphQLResponseBody>().await.unwrap().into();

        match (
            pretty_response_body.errors,
            pretty_response_body.data,
            expected_response,
        ) {
            (Some(actual_error), None, Err(expected_error)) => {
                assert_eq!(
                    expected_error,
                    // Let's add \n for the sake of convenience of passing the expected result
                    format!("{actual_error}\n")
                );
            }
            (None, Some(actual_response), Ok(expected_response)) => {
                // Let's add \n for the sake of convenience of passing the expected result
                assert_eq!(expected_response, format!("{actual_response}\n"));
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

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////
