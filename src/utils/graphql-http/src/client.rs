// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use internal_error::{ErrorIntoInternal, InternalError, ResultIntoInternal, root_source_message};
use serde::Deserialize;
use serde_json::Value as JsonValue;
use tracing::{Instrument, field};
use url::Url;

use crate::GraphqlHttpRequestError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct GraphqlHttpClient {
    endpoint_url: Url,
    http_client: reqwest::Client,
    maybe_access_token: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl GraphqlHttpClient {
    pub fn new(endpoint_url: Url, maybe_access_token: Option<String>) -> Self {
        Self {
            endpoint_url,
            http_client: reqwest::Client::new(),
            maybe_access_token,
        }
    }

    pub fn from_backend_url(backend_url: &Url, maybe_access_token: Option<String>) -> Self {
        Self::new(
            Self::endpoint_url_from_backend_url(backend_url),
            maybe_access_token,
        )
    }

    pub fn endpoint_url_from_backend_url(backend_url: &Url) -> Url {
        let mut endpoint_url = backend_url.clone();
        let path = endpoint_url.path().trim_end_matches('/');
        let path = if path.is_empty() {
            "/graphql".to_string()
        } else {
            format!("{path}/graphql")
        };
        endpoint_url.set_path(&path);
        endpoint_url.set_query(None);
        endpoint_url.set_fragment(None);
        endpoint_url
    }

    pub fn endpoint_url(&self) -> &Url {
        &self.endpoint_url
    }

    pub async fn execute<T>(&self, query: &str) -> Result<T, GraphqlHttpRequestError>
    where
        T: serde::de::DeserializeOwned,
    {
        let body = serde_json::to_vec(&serde_json::json!({ "query": query })).int_err()?;

        self.execute_body(body).await
    }

    pub async fn execute_operation<T, V>(
        &self,
        operation: cynic::Operation<T, V>,
    ) -> Result<T, GraphqlHttpRequestError>
    where
        T: serde::de::DeserializeOwned,
        V: serde::Serialize,
    {
        let body = serde_json::to_vec(&operation).int_err()?;

        self.execute_body(body).await
    }

    async fn execute_body<T>(&self, body: Vec<u8>) -> Result<T, GraphqlHttpRequestError>
    where
        T: serde::de::DeserializeOwned,
    {
        let span = tracing::info_span!(
            "GraphqlHttpClient_execute",
            endpoint = %self.endpoint_url,
            has_access_token = self.maybe_access_token.is_some(),
            http_status = field::Empty,
        );

        let mut request = self
            .http_client
            .post(self.endpoint_url.clone())
            .header(ACCEPT, "application/json")
            .header(CONTENT_TYPE, "application/json")
            .body(body);

        if let Some(access_token) = self.maybe_access_token.as_ref() {
            request = request.header(AUTHORIZATION, format!("Bearer {access_token}"));
        }

        let response = request
            .send()
            .instrument(span.clone())
            .await
            .map_err(|error| {
                tracing::error!(
                    error = %error,
                    endpoint = %self.endpoint_url,
                    "Remote GraphQL request failed during transport"
                );
                GraphqlHttpRequestError::transport(
                    self.endpoint_url.clone(),
                    root_source_message(&error),
                )
            })?;

        let status = response.status();
        span.record("http_status", status.as_u16());
        let response_body = response.text().await.map_err(|error| {
            tracing::error!(
                error = %error,
                endpoint = %self.endpoint_url,
                "Failed to read remote GraphQL response body"
            );
            GraphqlHttpRequestError::transport(
                self.endpoint_url.clone(),
                root_source_message(&error),
            )
        })?;

        if !status.is_success() {
            tracing::error!(
                endpoint = %self.endpoint_url,
                http_status = status.as_u16(),
                "Remote GraphQL request returned non-success status"
            );
            return Err(GraphqlHttpRequestError::http_status(
                self.endpoint_url.clone(),
                status,
            ));
        }

        let response: GraphqlResponse = serde_json::from_str(&response_body).map_err(|error| {
            tracing::error!(
                error = %error,
                endpoint = %self.endpoint_url,
                "Failed to deserialize remote GraphQL response"
            );
            error.int_err()
        })?;

        if let Some(errors) = response.errors
            && !errors.is_empty()
        {
            let message = errors
                .into_iter()
                .map(|error| error.message)
                .collect::<Vec<_>>()
                .join("; ");

            tracing::error!(
                endpoint = %self.endpoint_url,
                message = %message,
                "Remote GraphQL request returned GraphQL errors"
            );
            return Err(GraphqlHttpRequestError::graphql(
                self.endpoint_url.clone(),
                message,
            ));
        }

        let data = response.data.ok_or_else(|| {
            tracing::error!(
                endpoint = %self.endpoint_url,
                "Remote GraphQL response returned no data"
            );
            GraphqlHttpRequestError::Internal(InternalError::new(format!(
                "Remote GraphQL response from '{}' returned no data",
                self.endpoint_url
            )))
        })?;

        serde_json::from_value(data).map_err(|error| {
            tracing::error!(
                error = %error,
                endpoint = %self.endpoint_url,
                "Failed to deserialize remote GraphQL response data"
            );
            GraphqlHttpRequestError::Internal(error.int_err().with_context(format!(
                "Failed to deserialize remote GraphQL response data from '{}'",
                self.endpoint_url
            )))
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Deserialize)]
struct GraphqlResponse {
    data: Option<JsonValue>,

    #[serde(default)]
    errors: Option<Vec<GraphqlError>>,
}

#[derive(Debug, Deserialize)]
struct GraphqlError {
    message: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
