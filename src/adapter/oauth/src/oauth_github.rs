// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use email_utils::Email;
use internal_error::ResultIntoInternal;
use kamu_accounts::*;
use serde::Deserialize;
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID: &str = "KAMU_AUTH_GITHUB_CLIENT_ID";
pub const ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET: &str = "KAMU_AUTH_GITHUB_CLIENT_SECRET";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct OAuthGithub {
    config: Arc<GithubAuthenticationConfig>,
}

#[component(pub)]
#[interface(dyn AuthenticationProvider)]
#[scope(Singleton)]
impl OAuthGithub {
    pub fn new(config: Arc<GithubAuthenticationConfig>) -> Self {
        Self { config }
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
        client: &reqwest::Client,
        code: String,
    ) -> Result<GithubAccountInfo, ProviderLoginError> {
        let params = [
            ("client_id", self.config.client_id.as_str()),
            ("client_secret", self.config.client_secret.as_str()),
            ("code", code.as_str()),
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

        self.github_login_via_access_token(client, &github_access_token.access_token)
            .await
    }

    async fn github_login_via_access_token(
        &self,
        client: &reqwest::Client,
        access_token: &String,
    ) -> Result<GithubAccountInfo, ProviderLoginError> {
        let mut github_account_info = client
            .get("https://api.github.com/user")
            .bearer_auth(access_token)
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

        // The user may not have a public email, request primary private email
        if github_account_info.email.is_none() {
            github_account_info.email =
                Some(self.fetch_user_primary_email(client, access_token).await?);
        }

        Ok(github_account_info)
    }

    async fn fetch_user_primary_email(
        &self,
        client: &reqwest::Client,
        access_token: &String,
    ) -> Result<String, ProviderLoginError> {
        // Note: the request goes without pagination (first 30 emails)
        let github_emails_response = client
            .get("https://api.github.com/user/emails")
            .bearer_auth(access_token)
            .header(http::header::ACCEPT, "application/vnd.github.v3+json")
            .send()
            .await
            .int_err()?
            .error_for_status()
            .map_err(|e| {
                ProviderLoginError::InvalidCredentials(InvalidCredentialsError::new(Box::new(e)))
            })?
            .json::<Vec<GithubEmailInfo>>()
            .await
            .int_err()?;

        let maybe_github_primary_email = github_emails_response
            .into_iter()
            .filter(|github_email_info| github_email_info.primary && github_email_info.verified)
            .map(|github_email_info| github_email_info.email)
            .next();

        maybe_github_primary_email
            .ok_or_else(|| ProviderLoginError::NoPrimaryEmail(NoPrimaryEmailError {}))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AuthenticationProvider for OAuthGithub {
    fn provider_name(&self) -> &'static str {
        AccountProvider::OAuthGitHub.into()
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
            self.github_login_via_code(&client, code).await?
        } else if let Some(access_token) = github_login_credentials.access_token {
            self.github_login_via_access_token(&client, &access_token)
                .await?
        } else {
            // Either "code" or "access_token" are expected in the query
            return Err(ProviderLoginError::InvalidCredentials(
                InvalidCredentialsError::new(Box::new(GithubInvalidCredentialsError {})),
            ));
        };

        // Validate email
        let email = Email::parse(
            github_account_info
                .email
                .expect("Email should be extracted by now")
                .as_str(),
        )
        .unwrap();

        // Extract matching fields
        Ok(ProviderLoginResponse {
            // For GitHub, generate a random DID, regardless of the name
            account_id: odf::AccountID::new_generated_ed25519().1,
            account_name: odf::AccountName::new_unchecked(&github_account_info.login),
            account_type: AccountType::User,
            email,
            display_name: github_account_info
                .name
                .unwrap_or(github_account_info.login),
            avatar_url: github_account_info.avatar_url,
            // Use GitHub ID as an identity key
            provider_identity_key: github_account_info.id.to_string(),
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GithubLoginCredentials {
    pub code: Option<String>,
    pub access_token: Option<String>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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

#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct GithubEmailInfo {
    pub email: String,
    pub primary: bool,
    pub verified: bool,
    pub visibility: Option<GithubEmailVisibility>,
}

#[derive(Debug, Clone, Deserialize)]
enum GithubEmailVisibility {
    #[serde(rename = "public")]
    Public,
    #[serde(rename = "private")]
    Private,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
#[error("Invalid credentials: pass either Github code or access token")]
struct GithubInvalidCredentialsError {}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct GithubAuthenticationConfig {
    pub client_id: String,
    pub client_secret: String,
}

impl GithubAuthenticationConfig {
    pub fn new(client_id: String, client_secret: String) -> Self {
        Self {
            client_id,
            client_secret,
        }
    }

    pub fn load_from_env() -> Self {
        // Check for empty values only when the server API is running -- for this
        // reason, it is acceptable to use default values for other cases
        Self {
            client_id: std::env::var(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID)
                .ok()
                .unwrap_or_default(),

            client_secret: std::env::var(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET)
                .ok()
                .unwrap_or_default(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AuthenticationProvider)]
pub struct DummyOAuthGithub {}

#[async_trait::async_trait]
impl AuthenticationProvider for DummyOAuthGithub {
    fn provider_name(&self) -> &'static str {
        AccountProvider::OAuthGitHub.into()
    }

    async fn login(
        &self,
        _login_credentials_json: String,
    ) -> Result<ProviderLoginResponse, ProviderLoginError> {
        let account = "e2e-user".to_string();

        Ok(ProviderLoginResponse {
            account_id: odf::AccountID::new_generated_ed25519().1,
            account_name: odf::AccountName::new_unchecked(&account),
            account_type: AccountType::User,
            email: Email::parse("e2e-user@example.com").unwrap(),
            display_name: account.clone(),
            avatar_url: None,
            provider_identity_key: account,
        })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
