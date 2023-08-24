// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_core::{InternalError, ResultIntoInternal};
use serde::Deserialize;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////

pub struct OAuthGithub {
    client_id: String,
    client_secret: String,
}

impl OAuthGithub {
    pub fn new() -> Self {
        let client_id = std::env::var("KAMU_AUTH_GITHUB_CLIENT_ID")
            .expect("KAMU_AUTH_GITHUB_CLIENT_ID env var is not set");
        let client_secret = std::env::var("KAMU_AUTH_GITHUB_CLIENT_SECRET")
            .expect("KAMU_AUTH_GITHUB_CLIENT_SECRET env var is not set");

        Self {
            client_id,
            client_secret,
        }
    }

    fn get_client(&self) -> Result<reqwest::Client, reqwest::Error> {
        reqwest::Client::builder()
            .user_agent(concat!(
                env!("CARGO_PKG_NAME"),
                "/",
                env!("CARGO_PKG_VERSION"),
            ))
            .build()
    }

    pub async fn login(&self, code: String) -> Result<GithubLoginResponse, GithubError> {
        let params = [
            ("client_id", self.client_id.clone()),
            ("client_secret", self.client_secret.clone()),
            ("code", code),
        ];

        let client = self.get_client().int_err()?;

        let body = client
            .post("https://github.com/login/oauth/access_token")
            .header(reqwest::header::ACCEPT, "application/json")
            .form(&params)
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?
            .text()
            .await
            .int_err()?;

        let token =
            serde_json::from_str::<GithubAccessToken>(&body).map_err(|_| GithubResponseError {
                github_response: body,
            })?;

        let account_info = client
            .get("https://api.github.com/user")
            .bearer_auth(&token.access_token)
            .header(reqwest::header::ACCEPT, "application/vnd.github.v3+json")
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?
            .json::<GithubAccountInfo>()
            .await
            .int_err()?;

        Ok(GithubLoginResponse {
            token,
            account_info,
        })
    }

    pub async fn account_info(
        &self,
        access_token: String,
    ) -> Result<GithubAccountInfo, GithubError> {
        let client = self.get_client().int_err()?;

        let account_info = client
            .get("https://api.github.com/user")
            .bearer_auth(access_token)
            .header(reqwest::header::ACCEPT, "application/vnd.github.v3+json")
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?
            .json::<GithubAccountInfo>()
            .await
            .int_err()?;

        Ok(account_info)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize)]
pub struct GithubLoginResponse {
    pub token: GithubAccessToken,
    pub account_info: GithubAccountInfo,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GithubAccessToken {
    pub access_token: String,
    pub scope: String,
    pub token_type: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GithubAccountInfo {
    pub login: String,
    pub name: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub gravatar_id: Option<String>,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GithubError {
    #[error(transparent)]
    GithubResponse(GithubResponseError),

    #[error(transparent)]
    Internal(InternalError),
}

impl From<InternalError> for GithubError {
    fn from(value: InternalError) -> Self {
        Self::Internal(value)
    }
}

impl From<GithubResponseError> for GithubError {
    fn from(value: GithubResponseError) -> Self {
        Self::GithubResponse(value)
    }
}

#[derive(Debug, Error)]
#[error("Github error: {github_response}")]
pub struct GithubResponseError {
    pub github_response: String,
}

///////////////////////////////////////////////////////////////////////////////
