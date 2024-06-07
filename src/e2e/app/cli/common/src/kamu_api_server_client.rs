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
use tokio_retry::strategy::FixedInterval;
use tokio_retry::Retry;

////////////////////////////////////////////////////////////////////////////////

// TODO: GQL-query method

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
}

////////////////////////////////////////////////////////////////////////////////
