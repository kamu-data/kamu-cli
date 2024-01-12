// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Mutex;

use dill::*;
use kamu_core::auth::{AccountInfo, AccountType};
use kamu_core::{ErrorIntoInternal, InternalError, ResultIntoInternal};
use opendatafabric::{AccountName, FAKE_ACCOUNT_ID};
use serde::{Deserialize, Serialize};

///////////////////////////////////////////////////////////////////////////////

const LOGIN_METHOD_GITHUB: &str = "oauth_github";

pub const ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID: &str = "KAMU_AUTH_GITHUB_CLIENT_ID";
pub const ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET: &str = "KAMU_AUTH_GITHUB_CLIENT_SECRET";

///////////////////////////////////////////////////////////////////////////////

pub struct OAuthGithub {
    cached_state: Mutex<CachedState>,
}

#[component(pub)]
#[interface(dyn kamu_core::auth::AuthenticationProvider)]
#[scope(Singleton)]
impl OAuthGithub {
    pub fn new() -> Self {
        Self {
            cached_state: Mutex::new(CachedState::new()),
        }
    }

    fn get_client_id() -> String {
        std::env::var(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID)
            .unwrap_or_else(|_| panic!("{} env var is not set", ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_ID))
    }

    fn get_client_secret() -> String {
        std::env::var(ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET).unwrap_or_else(|_| {
            panic!(
                "{} env var is not set",
                ENV_VAR_KAMU_AUTH_GITHUB_CLIENT_SECRET
            )
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

        let github_access_token =
            serde_json::from_str::<GithubAccessToken>(&body).map_err(|_| {
                kamu_core::auth::ProviderLoginError::RejectedCredentials(
                    kamu_core::auth::RejectedCredentialsError::new(body),
                )
            })?;

        let github_account_info = client
            .get("https://api.github.com/user")
            .bearer_auth(&github_access_token.access_token)
            .header(http::header::ACCEPT, "application/vnd.github.v3+json")
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?
            .json::<GithubAccountInfo>()
            .await
            .int_err()?;

        self.store_token_account_info_in_cache(
            &github_access_token.access_token,
            &github_account_info,
        );

        Ok(GithubLoginResponse {
            token: github_access_token,
            account_info: github_account_info,
        })
    }

    async fn github_own_account_info_impl(
        &self,
        access_token: String,
    ) -> Result<GithubAccountInfo, InternalError> {
        if let Some(github_account_info) = self.find_access_token_resolution_in_cache(&access_token)
        {
            return Ok(github_account_info);
        }

        let client = self.get_client().int_err()?;

        let github_account_info = client
            .get("https://api.github.com/user")
            .bearer_auth(access_token.clone())
            .header(http::header::ACCEPT, "application/vnd.github.v3+json")
            .send()
            .await
            .int_err()?
            .error_for_status()
            .int_err()?
            .json::<GithubAccountInfo>()
            .await
            .int_err()?;

        self.store_token_account_info_in_cache(&access_token, &github_account_info);

        Ok(github_account_info)
    }

    async fn github_find_account_info_by_login(
        &self,
        login: &String,
    ) -> Result<Option<GithubAccountInfo>, InternalError> {
        if let Some(github_account_info) = self.find_github_account_info_in_cache(login) {
            return Ok(Some(github_account_info));
        }

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
            let github_account_info = response.json::<GithubAccountInfo>().await.int_err()?;
            self.store_github_account_info_in_cache(&github_account_info);
            Ok(Some(github_account_info))
        }
    }

    fn store_github_account_info_in_cache(&self, github_account_info: &GithubAccountInfo) {
        let mut cached_state = self
            .cached_state
            .lock()
            .expect("Could not lock cached state");

        cached_state.save_account_info(github_account_info.clone());
    }

    fn store_token_account_info_in_cache(
        &self,
        access_token: &String,
        github_account_info: &GithubAccountInfo,
    ) {
        let mut cached_state = self
            .cached_state
            .lock()
            .expect("Could not lock cached state");

        cached_state
            .save_access_token_resolution(access_token.clone(), github_account_info.clone());
    }

    fn find_github_account_info_in_cache(&self, login: &String) -> Option<GithubAccountInfo> {
        let cached_state = self
            .cached_state
            .lock()
            .expect("Could not lock cached state");

        cached_state.find_account_info_by_login(login)
    }

    fn find_access_token_resolution_in_cache(
        &self,
        access_token: &String,
    ) -> Option<GithubAccountInfo> {
        let cached_state = self
            .cached_state
            .lock()
            .expect("Could not lock cached state");

        cached_state.find_access_token_resolution(access_token)
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
            serde_json::from_str::<GithubProviderCredentials>(provider_credentials_json.as_str())
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

struct CachedState {
    accounts_by_login: HashMap<String, GithubAccountInfo>,
    accounts_by_access_token: HashMap<String, GithubAccountInfo>,
}

impl CachedState {
    pub fn new() -> Self {
        Self {
            accounts_by_login: HashMap::new(),
            accounts_by_access_token: HashMap::new(),
        }
    }

    pub fn save_account_info(&mut self, github_account_info: GithubAccountInfo) {
        self.accounts_by_login
            .insert(github_account_info.login.clone(), github_account_info);
    }

    pub fn save_access_token_resolution(
        &mut self,
        access_token: String,
        github_account_info: GithubAccountInfo,
    ) {
        self.accounts_by_access_token
            .insert(access_token, github_account_info.clone());

        self.save_account_info(github_account_info);
    }

    pub fn find_account_info_by_login(&self, login: &String) -> Option<GithubAccountInfo> {
        self.accounts_by_login.get(login).cloned()
    }

    pub fn find_access_token_resolution(&self, access_token: &String) -> Option<GithubAccountInfo> {
        self.accounts_by_access_token.get(access_token).cloned()
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

impl From<GithubAccountInfo> for AccountInfo {
    fn from(value: GithubAccountInfo) -> Self {
        Self {
            account_id: FAKE_ACCOUNT_ID.to_string(),
            account_name: AccountName::try_from(&value.login).unwrap(),
            account_type: AccountType::User,
            display_name: value.name.unwrap_or(value.login),
            avatar_url: value.avatar_url,
            is_admin: false,
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
