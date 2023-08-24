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

pub(crate) struct AuthMut;

#[Object]
impl AuthMut {
    async fn password_login(
        &self,
        ctx: &Context<'_>,
        login: String,
        password: String,
    ) -> Result<LoginResponse> {
        let authentication_service =
            from_catalog::<dyn kamu_core::auth::AuthenticationService>(ctx).unwrap();

        let credentials = kamu_core::auth::PasswordLoginCredentials { login, password };

        let login_result = authentication_service
            .login(
                kamu_core::auth::LOGIN_METHOD_PASSWORD,
                serde_json::to_string::<kamu_core::auth::PasswordLoginCredentials>(&credentials)
                    .int_err()?,
            )
            .await;

        match login_result {
            Ok(login_response) => Ok(login_response.into()),
            Err(e) => Err(e.into()),
        }
    }

    async fn github_login(&self, ctx: &Context<'_>, code: String) -> Result<LoginResponse> {
        let authentication_service =
            from_catalog::<dyn kamu_core::auth::AuthenticationService>(ctx).unwrap();

        let credentials = kamu_core::auth::GithubLoginCredentials { code };

        let login_result = authentication_service
            .login(
                kamu_core::auth::LOGIN_METHOD_GITHUB,
                serde_json::to_string::<kamu_core::auth::GithubLoginCredentials>(&credentials)
                    .int_err()?,
            )
            .await;

        match login_result {
            Ok(login_response) => Ok(login_response.into()),
            Err(e) => Err(e.into()),
        }
    }

    async fn account_info(&self, ctx: &Context<'_>, access_token: String) -> Result<AccountInfo> {
        let authentication_service =
            from_catalog::<dyn kamu_core::auth::AuthenticationService>(ctx).unwrap();

        let get_account_info_result = authentication_service.get_account_info(access_token).await;
        match get_account_info_result {
            Ok(github_account_info) => Ok(github_account_info.into()),
            Err(e) => Err(e.into()),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

impl From<kamu_core::auth::LoginError> for GqlError {
    fn from(value: kamu_core::auth::LoginError) -> Self {
        match value {
            kamu_core::auth::LoginError::UnknownMethod(e) => GqlError::Internal(e.int_err()),
            kamu_core::auth::LoginError::InvalidCredentials(e) => GqlError::Internal(e.int_err()),
            kamu_core::auth::LoginError::RejectedCredentials(e) => GqlError::Gql(
                Error::new("Rejected credentials")
                    .extend_with(|_, eev| eev.set("reason", e.to_string())),
            ),
            kamu_core::auth::LoginError::Internal(e) => GqlError::Internal(e),
        }
    }
}

impl From<kamu_core::auth::GetAccountInfoError> for GqlError {
    fn from(value: kamu_core::auth::GetAccountInfoError) -> Self {
        match value {
            kamu_core::auth::GetAccountInfoError::AccessToken(e) => GqlError::Gql(
                Error::new("Access token error")
                    .extend_with(|_, eev| eev.set("token_error", e.to_string())),
            ),
            kamu_core::auth::GetAccountInfoError::Internal(e) => GqlError::Internal(e),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub(crate) struct LoginResponse {
    access_token: String,
    account_info: AccountInfo,
}

impl From<kamu_core::auth::LoginResponse> for LoginResponse {
    fn from(value: kamu_core::auth::LoginResponse) -> Self {
        Self {
            access_token: value.access_token.into(),
            account_info: value.account_info.into(),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
pub(crate) struct AccountInfo {
    login: AccountName,
    name: String,
    avatar_url: Option<String>,
}

impl From<kamu_core::auth::AccountInfo> for AccountInfo {
    fn from(value: kamu_core::auth::AccountInfo) -> Self {
        Self {
            login: AccountName::from(value.login),
            name: value.name,
            avatar_url: value.avatar_url,
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
