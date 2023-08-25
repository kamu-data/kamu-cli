// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::component;
use kamu_core::{InternalError, ResultIntoInternal};
use opendatafabric::AccountName;
use serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////

const LOGIN_METHOD_GITHUB: &str = "oauth_github";

///////////////////////////////////////////////////////////////////////////////

pub struct OAuthGithub {
    client_id: String,
    client_secret: String,
}

#[component(pub)]
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

    async fn github_login_impl(
        &self,
        code: String,
    ) -> Result<GithubLoginResponse, kamu_core::auth::ProviderLoginError> {
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

        let token = serde_json::from_str::<GithubAccessToken>(&body).map_err(|_| {
            kamu_core::auth::ProviderLoginError::RejectedCredentials(
                kamu_core::auth::RejectedCredentialsError::new(body.into()),
            )
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

    async fn github_account_info_impl(
        &self,
        access_token: String,
    ) -> Result<GithubAccountInfo, InternalError> {
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

#[async_trait::async_trait]
impl kamu_core::auth::AuthenticationProvider for OAuthGithub {
    fn login_method(&self) -> &'static str {
        LOGIN_METHOD_GITHUB
    }

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<kamu_core::auth::ProviderLoginResponse, kamu_core::auth::ProviderLoginError> {
        // Decode credentials
        let github_login_credentials =
            serde_json::from_str::<GithubLoginCredentials>(login_credentials_json.as_str())
                .map_err(|e| {
                    kamu_core::auth::ProviderLoginError::InvalidCredentials(
                        kamu_core::auth::InvalidCredentialsError::new(Box::new(e)),
                    )
                })?;

        // Access GitHub OAuth Api
        let github_login_response = self
            .github_login_impl(github_login_credentials.code)
            .await?;

        // Prepare results
        let provider_credentials = GithubProviderCredentials {
            access_token: github_login_response.token.access_token,
        };
        Ok(kamu_core::auth::ProviderLoginResponse {
            provider_credentials_json: serde_json::to_string::<GithubProviderCredentials>(
                &provider_credentials,
            )
            .int_err()?,
            account_info: github_login_response.account_info.into(),
        })
    }

    async fn get_account_info(
        &self,
        provider_credentials_json: String,
    ) -> Result<kamu_core::auth::AccountInfo, InternalError> {
        let provider_credentials =
            serde_json::from_str::<GithubProviderCredentials>(&provider_credentials_json.as_str())
                .int_err()?;

        let account_info = self
            .github_account_info_impl(provider_credentials.access_token)
            .await
            .int_err()?;

        Ok(account_info.into())
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize)]
pub struct GithubLoginCredentials {
    pub code: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GithubProviderCredentials {
    pub access_token: String,
}

impl From<GithubAccountInfo> for kamu_core::auth::AccountInfo {
    fn from(value: GithubAccountInfo) -> Self {
        Self {
            login: AccountName::try_from(&value.login).unwrap(),
            name: value.name,
            avatar_url: value.avatar_url,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize)]
struct GithubLoginResponse {
    pub token: GithubAccessToken,
    pub account_info: GithubAccountInfo,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GithubAccessToken {
    pub access_token: String,
    pub scope: String,
    pub token_type: String,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct GithubAccountInfo {
    pub login: String,
    pub name: String,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub gravatar_id: Option<String>,
}

///////////////////////////////////////////////////////////////////////////////
