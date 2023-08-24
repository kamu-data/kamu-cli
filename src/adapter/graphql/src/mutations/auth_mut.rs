// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct AuthMut {
    github_oauth: kamu_adapter_oauth::OAuthGithub,
}

#[Object]
impl AuthMut {
    #[graphql(skip)]
    pub fn new() -> Self {
        Self {
            github_oauth: kamu_adapter_oauth::OAuthGithub::new(),
        }
    }

    async fn password_login(&self, login: String, password: String) -> Result<LoginResponse> {
        tracing::info!(%login, %password, "Password login"); // TODO: don't log password
        unimplemented!("Password login not supported yet");
    }

    async fn github_login(&self, code: String) -> Result<LoginResponse> {
        // TODO: output token will be wrapped with our own JWT
        match self.github_oauth.login(code).await {
            Ok(github_login_response) => Ok(github_login_response.into()),
            Err(e) => Err(e.into()),
        }
    }

    async fn account_info(&self, access_token: String) -> Result<AccountInfo> {
        // TODO: access token will be our own JWT, so deciding whether it is for Github
        // would not be done here
        match self.github_oauth.account_info(access_token).await {
            Ok(github_account_info) => Ok(github_account_info.into()),
            Err(e) => Err(e.into()),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

impl From<kamu_adapter_oauth::GithubError> for GqlError {
    fn from(value: kamu_adapter_oauth::GithubError) -> Self {
        match value {
            kamu_adapter_oauth::GithubError::GithubResponse(github_response_error) => {
                GqlError::Gql(
                    Error::new("Failed to process auth response").extend_with(|_, e| {
                        e.set("github_response", github_response_error.github_response)
                    }),
                )
            }
            kamu_adapter_oauth::GithubError::Internal(e) => GqlError::Internal(e),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub(crate) struct LoginResponse {
    access_token: String,
    account_info: AccountInfo,
}

impl From<kamu_adapter_oauth::GithubLoginResponse> for LoginResponse {
    fn from(value: kamu_adapter_oauth::GithubLoginResponse) -> Self {
        Self {
            access_token: value.token.access_token.into(),
            account_info: value.account_info.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

// TODO: reduce the structure to avoid Github specifics
#[derive(SimpleObject, Debug, Clone)]
pub(crate) struct AccountInfo {
    login: String,
    name: String,
    email: Option<String>,
    avatar_url: Option<String>,
    gravatar_id: Option<String>,
}

impl From<kamu_adapter_oauth::GithubAccountInfo> for AccountInfo {
    fn from(value: kamu_adapter_oauth::GithubAccountInfo) -> Self {
        Self {
            login: value.login,
            name: value.name,
            email: value.email,
            avatar_url: value.avatar_url,
            gravatar_id: value.gravatar_id,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
