// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::RevokeTokenError;

use crate::prelude::*;
use crate::queries::{Account, CreatedAccessToken};
use crate::utils::{check_access_token_valid, check_logged_account_id_match};

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
            from_catalog::<dyn kamu_accounts::AuthenticationService>(ctx).unwrap();

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
            from_catalog::<dyn kamu_accounts::AuthenticationService>(ctx).unwrap();

        match authentication_service.account_by_token(access_token).await {
            Ok(a) => Ok(Account::from_account(a)),
            Err(e) => Err(e.into()),
        }
    }

    async fn create_access_token(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID,
        token_name: String,
    ) -> Result<CreatedAccessToken> {
        check_logged_account_id_match(ctx, &account_id)?;

        let access_token_service =
            from_catalog::<dyn kamu_accounts::AccessTokenService>(ctx).unwrap();

        let created_token = access_token_service
            .create_access_token(&token_name, &account_id)
            .await
            .int_err()?;

        Ok(CreatedAccessToken::new(
            created_token,
            &account_id,
            &token_name,
        ))
    }

    async fn revoke_access_token(
        &self,
        ctx: &Context<'_>,
        token_id: AccessTokenID,
    ) -> Result<RevokeResult> {
        check_access_token_valid(ctx, &token_id).await?;

        let access_token_service =
            from_catalog::<dyn kamu_accounts::AccessTokenService>(ctx).unwrap();

        match access_token_service
            .revoke_access_token(&token_id.into())
            .await
        {
            Ok(_) => Ok(RevokeResult::Success),
            Err(RevokeTokenError::AlreadyRevoked) => Ok(RevokeResult::AlreadyRevoked),
            Err(RevokeTokenError::NotFound(_)) => Err(GqlError::Gql(async_graphql::Error::new(
                "Access token not found",
            ))),
            Err(RevokeTokenError::Internal(e)) => Err(e.into()),
        }
    }
}

///////////////////////////////////////////////////////////////////////////////

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
            kamu_accounts::LoginError::DuplicateCredentials => {
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

///////////////////////////////////////////////////////////////////////////////

#[derive(SimpleObject, Debug, Clone)]
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

///////////////////////////////////////////////////////////////////////////////

#[derive(Enum, Debug, Clone, Copy, Eq, PartialEq)]
pub enum RevokeResult {
    Success,
    AlreadyRevoked,
    NotFound,
}
