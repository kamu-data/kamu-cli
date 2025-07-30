// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::mutations::AuthWeb3Mut;
use crate::prelude::*;
use crate::queries::Account;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct AuthMut;

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl AuthMut {
    /// Web3-related functionality group
    async fn web3(&self) -> AuthWeb3Mut {
        AuthWeb3Mut
    }

    #[tracing::instrument(level = "info", name = AuthMut_login, skip_all, fields(?login_method))]
    async fn login(
        &self,
        ctx: &Context<'_>,
        login_method: AccountProvider,
        login_credentials_json: String,
        device_code: Option<DeviceCode<'_>>,
    ) -> Result<LoginResponse> {
        let authentication_service = from_catalog_n!(ctx, dyn kamu_accounts::AuthenticationService);

        let login_method: kamu_accounts::AccountProvider = login_method.into();
        let device_code = device_code.map(Into::into);
        let login_result = authentication_service
            .login(login_method.into(), login_credentials_json, device_code)
            .await;

        match login_result {
            Ok(login_response) => Ok(login_response.into()),
            Err(e) => Err(e.into()),
        }
    }

    #[tracing::instrument(level = "info", name = AuthMut_account_details, skip_all)]
    async fn account_details(&self, ctx: &Context<'_>, access_token: String) -> Result<Account> {
        let authentication_service = from_catalog_n!(ctx, dyn kamu_accounts::AuthenticationService);

        match authentication_service.account_by_token(access_token).await {
            Ok(a) => Ok(Account::from_account(a)),
            Err(e) => Err(e.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl From<kamu_accounts::LoginError> for GqlError {
    fn from(value: kamu_accounts::LoginError) -> Self {
        match value {
            kamu_accounts::LoginError::UnsupportedMethod(e) => GqlError::Gql(
                Error::new(e.to_string()).extend_with(|_, eev| eev.set("method", e.to_string())),
            ),
            kamu_accounts::LoginError::InvalidCredentials(e) => GqlError::Gql(
                Error::new(e.to_string()).extend_with(|_, eev| eev.set("reason", e.to_string())),
            ),
            kamu_accounts::LoginError::RejectedCredentials(e) => GqlError::Gql(
                Error::new(e.to_string()).extend_with(|_, eev| eev.set("reason", e.to_string())),
            ),
            kamu_accounts::LoginError::NoPrimaryEmail(e) => GqlError::Gql(
                Error::new(e.to_string()).extend_with(|_, eev| eev.set("reason", e.to_string())),
            ),
            kamu_accounts::LoginError::DuplicateCredentials
            | kamu_accounts::LoginError::RestrictedLogin => {
                GqlError::Gql(Error::new(value.to_string()))
            }
            kamu_accounts::LoginError::Internal(e) => GqlError::Internal(e),
        }
    }
}

impl From<kamu_accounts::GetAccountInfoError> for GqlError {
    fn from(value: kamu_accounts::GetAccountInfoError) -> Self {
        match value {
            kamu_accounts::GetAccountInfoError::AccessToken(e) => GqlError::Gql(
                Error::new("Access token error")
                    .extend_with(|_, eev| eev.set("token_error", e.to_string())),
            ),
            kamu_accounts::GetAccountInfoError::AccountUnresolved => GqlError::Gql(Error::new(
                "Access token error: pointed account does not exist",
            )),
            kamu_accounts::GetAccountInfoError::Internal(e) => GqlError::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug)]
pub(crate) struct LoginResponse {
    access_token: String,
    account: Account,
}

impl From<kamu_accounts::LoginResponse> for LoginResponse {
    fn from(value: kamu_accounts::LoginResponse) -> Self {
        Self {
            access_token: value.access_token,
            account: Account::new(value.account_id.into(), value.account_name.into()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
