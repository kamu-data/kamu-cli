// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::prelude::*;
use crate::queries::Account;

///////////////////////////////////////////////////////////////////////////////

pub(crate) struct AuthMut;

#[Object]
impl AuthMut {
    async fn login(
        &self,
        ctx: &Context<'_>,
        login_method: String,
        login_credentials_json: String,
    ) -> Result<LoginResponse> {
        let authentication_service =
            from_catalog::<dyn kamu_core::auth::AuthenticationService>(ctx).unwrap();

        let login_result = authentication_service
            .login(login_method.as_str(), login_credentials_json)
            .await;

        match login_result {
            Ok(login_response) => Ok(login_response.into()),
            Err(e) => Err(e.into()),
        }
    }

    async fn account_details(&self, ctx: &Context<'_>, access_token: String) -> Result<Account> {
        let authentication_service =
            from_catalog::<dyn kamu_core::auth::AuthenticationService>(ctx).unwrap();

        let get_account_info_result = authentication_service
            .account_info_by_token(access_token)
            .await;
        match get_account_info_result {
            Ok(ai) => Ok(Account::new(ai)),
            Err(e) => Err(e.into()),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

impl From<kamu_core::auth::LoginError> for GqlError {
    fn from(value: kamu_core::auth::LoginError) -> Self {
        match value {
            kamu_core::auth::LoginError::UnsupportedMethod(e) => GqlError::Gql(
                Error::new(e.to_string()).extend_with(|_, eev| eev.set("method", e.to_string())),
            ),
            kamu_core::auth::LoginError::InvalidCredentials(e) => GqlError::Gql(
                Error::new(e.to_string()).extend_with(|_, eev| eev.set("reason", e.to_string())),
            ),
            kamu_core::auth::LoginError::RejectedCredentials(e) => GqlError::Gql(
                Error::new(e.to_string()).extend_with(|_, eev| eev.set("reason", e.to_string())),
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
    account: Account,
}

impl From<kamu_core::auth::LoginResponse> for LoginResponse {
    fn from(value: kamu_core::auth::LoginResponse) -> Self {
        Self {
            access_token: value.access_token.into(),
            account: Account::new(value.account_info),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////
