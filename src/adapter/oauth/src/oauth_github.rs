// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::component;
use kamu_core::auth::{AccountInfo, AccountType};
use kamu_core::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use opendatafabric::{AccountName, FAKE_ACCOUNT_ID};
use serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////

const LOGIN_METHOD_GITHUB: &str = "oauth_github";

///////////////////////////////////////////////////////////////////////////////

pub struct OAuthGithub;

#[component(pub)]
impl OAuthGithub {
    pub fn new() -> Self {
        Self {}
    }

    fn get_client_id() -> String {
        std::env::var("KAMU_AUTH_GITHUB_CLIENT_ID")
            .expect("KAMU_AUTH_GITHUB_CLIENT_ID env var is not set")
    }

    fn get_client_secret() -> String {
        std::env::var("KAMU_AUTH_GITHUB_CLIENT_SECRET")
            .expect("KAMU_AUTH_GITHUB_CLIENT_SECRET env var is not set")
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
            ("client_id", OAuthGithub::get_client_id()),
            ("client_secret", OAuthGithub::get_client_secret()),
            ("code", code),
        ];

        let client = self.get_client().int_err()?;

        let body = client
            .post("https://github.com/login/oauth/access_token")
            .header(http::header::ACCEPT, "application/json")
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
            .header(http::header::ACCEPT, "application/vnd.github.v3+json")
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

    async fn github_own_account_info_impl(
        &self,
        access_token: String,
    ) -> Result<GithubAccountInfo, InternalError> {
        let client = self.get_client().int_err()?;

        let account_info = client
            .get("https://api.github.com/user")
            .bearer_auth(access_token)
            .header(http::header::ACCEPT, "application/vnd.github.v3+json")
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

    async fn github_find_account_info_by_login(
        &self,
        login: &String,
    ) -> Result<Option<GithubAccountInfo>, InternalError> {
        let client = self.get_client().int_err()?;

        let response = client
            .get(format!("https://api.github.com/users/{}", login))
            .header(http::header::ACCEPT, "application/vnd.github.v3+json")
            .send()
            .await
            .int_err()?;

        if response.status().is_client_error() {
            Ok(None)
        } else if response.status().is_server_error() {
            Err(response.error_for_status().unwrap_err().int_err())
        } else {
            let account_info = response.json::<GithubAccountInfo>().await.int_err()?;
            Ok(Some(account_info))
        }
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

    async fn account_info_by_token(
        &self,
        provider_credentials_json: String,
    ) -> Result<kamu_core::auth::AccountInfo, InternalError> {
        let provider_credentials =
            serde_json::from_str::<GithubProviderCredentials>(&provider_credentials_json.as_str())
                .int_err()?;

        let account_info = self
            .github_own_account_info_impl(provider_credentials.access_token)
            .await
            .int_err()?;

        Ok(account_info.into())
    }

    async fn find_account_info_by_name<'a>(
        &'a self,
        account_name: &'a AccountName,
    ) -> Result<Option<AccountInfo>, InternalError> {
        let github_login = account_name.to_string();
        let maybe_account_info = self
            .github_find_account_info_by_login(&github_login)
            .await?;
        Ok(maybe_account_info.map(|github_ai| github_ai.into()))
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
            account_id: FAKE_ACCOUNT_ID.to_string(),
            account_name: AccountName::try_from(&value.login).unwrap(),
            account_type: AccountType::User,
            display_name: value.name.or_else(|| Some(value.login)).unwrap(),
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
    pub name: Option<String>,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub gravatar_id: Option<String>,
}

///////////////////////////////////////////////////////////////////////////////
