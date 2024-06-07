// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::{InternalError, ResultIntoInternal};
use reqwest::{StatusCode, Url};
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
        let retry_strategy = FixedInterval::from_millis(250).take(10);
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

    pub async fn api_call_assert(&self, query: &str, expected_response: Result<&str, &str>) {
        let endpoint = self.server_base_url.join("graphql").unwrap();
        let request_data = serde_json::json!({
           "query": query
        });

        let response = self
            .http_client
            .post(endpoint)
            .json(&request_data)
            .send()
            .await
            .unwrap();

        assert_eq!(StatusCode::OK, response.status());

        let response_body: ResponsePrettyBody =
            response.json::<ResponseBody>().await.unwrap().into();

        match (response_body.errors, response_body.data, expected_response) {
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
struct ResponseBody {
    data: Option<serde_json::Value>,
    errors: Option<serde_json::Value>,
}

struct ResponsePrettyBody {
    data: Option<String>,
    errors: Option<String>,
}

impl From<ResponseBody> for ResponsePrettyBody {
    fn from(value: ResponseBody) -> Self {
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
