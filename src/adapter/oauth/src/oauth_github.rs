// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use kamu_accounts::*;
use kamu_core::ResultIntoInternal;
use opendatafabric::{AccountID, AccountName};
use serde::Deserialize;
use thiserror::Error;

///////////////////////////////////////////////////////////////////////////////

pub const PROVIDER_GITHUB: &str = "oauth_github";

pub const ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID: &str = "KAMU_AUTH_GITHUB_CLIENT_ID";
pub const ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET: &str = "KAMU_AUTH_GITHUB_CLIENT_SECRET";

///////////////////////////////////////////////////////////////////////////////

pub struct OAuthGithub {}

#[component(pub)]
#[interface(dyn AuthenticationProvider)]
#[scope(Singleton)]
impl OAuthGithub {
    pub fn new() -> Self {
        Self {}
    }

    fn get_client_id() -> String {
        std::env::var(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID)
            .unwrap_or_else(|_| panic!("{ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID} env var is not set"))
    }

    fn get_client_secret() -> String {
        std::env::var(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET).unwrap_or_else(|_| {
            panic!("{ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET} env var is not set")
        })
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

    async fn github_login_via_code(
        &self,
        client: reqwest::Client,
        code: String,
    ) -> Result<GithubAccountInfo, ProviderLoginError> {
        let params = [
            ("client_id", OAuthGithub::get_client_id()),
            ("client_secret", OAuthGithub::get_client_secret()),
            ("code", code),
        ];

        let body = client
            .post("https://github.com/login/oauth/access_token")
            .header(http::header::ACCEPT, "application/json")
            .form(&params)
            .send()
            .await
            .int_err()?
            .error_for_status()
            .map_err(|e| {
                ProviderLoginError::InvalidCredentials(InvalidCredentialsError::new(Box::new(e)))
            })?
            .text()
            .await
            .int_err()?;

        let github_access_token = serde_json::from_str::<GithubAccessToken>(&body)
            .map_err(|_| ProviderLoginError::RejectedCredentials(RejectedCredentialsError {}))?;

        self.github_login_via_access_token(client, github_access_token.access_token)
            .await
    }

    async fn github_login_via_access_token(
        &self,
        client: reqwest::Client,
        access_token: String,
    ) -> Result<GithubAccountInfo, ProviderLoginError> {
        let github_account_info = client
            .get("https://api.github.com/user")
            .bearer_auth(&access_token)
            .header(http::header::ACCEPT, "application/vnd.github.v3+json")
            .send()
            .await
            .int_err()?
            .error_for_status()
            .map_err(|e| {
                ProviderLoginError::InvalidCredentials(InvalidCredentialsError::new(Box::new(e)))
            })?
            .json::<GithubAccountInfo>()
            .await
            .int_err()?;

        Ok(github_account_info)
    }
}

///////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AuthenticationProvider for OAuthGithub {
    fn provider_name(&self) -> &'static str {
        PROVIDER_GITHUB
    }

    fn generate_id(&self, _: &AccountName) -> AccountID {
        // For GitHub, generate a random DID, regardless of the name
        AccountID::new_generated_ed25519().1
    }

    async fn login(
        &self,
        login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        // Decode credentials
        let github_login_credentials =
            serde_json::from_str::<GithubLoginCredentials>(login_credentials_json.as_str())
                .map_err(|e| {
                    ProviderLoginError::InvalidCredentials(InvalidCredentialsError::new(Box::new(
                        e,
                    )))
                })?;

        // Prepare HTTP client for GitHub
        let client = self.get_client().int_err()?;

        // 2 types of login:
        //  - we have GitHub code, which we use to resolve the access token, and then
        //    use the token to get user info (UI flow)
        //  - we have GitHub access token already, which we use to get the user info
        //    quicker (silent login flow)
        let github_account_info = if let Some(code) = github_login_credentials.code {
            self.github_login_via_code(client, code).await?
        } else if let Some(access_token) = github_login_credentials.access_token {
            self.github_login_via_access_token(client, access_token)
                .await?
        } else {
            // Either "code" or "access_token" are expected in the query
            return Err(ProviderLoginError::InvalidCredentials(
                InvalidCredentialsError::new(Box::new(GithubInvalidCredentialsError {})),
            ));
        };

        // Extract matching fields
        Ok(ProviderLoginResponse {
            account_name: AccountName::new_unchecked(&github_account_info.login),
            account_type: AccountType::User,
            email: github_account_info.email,
            display_name: github_account_info
                .name
                .unwrap_or(github_account_info.login),
            avatar_url: github_account_info.avatar_url,
            // Use GitHub ID as an identity key
            provider_identity_key: github_account_info.id.to_string(),
        })
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GithubLoginCredentials {
    pub code: Option<String>,
    pub access_token: Option<String>,
}

///////////////////////////////////////////////////////////////////////////////

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
    pub id: i64,
    pub name: Option<String>,
    pub email: Option<String>,
    pub avatar_url: Option<String>,
    pub gravatar_id: Option<String>,
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Invalid credentials: pass either Github code or access token")]
struct GithubInvalidCredentialsError {}

///////////////////////////////////////////////////////////////////////////////
