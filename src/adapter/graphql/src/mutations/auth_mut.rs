// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{CreateAccessTokenError, RevokeTokenError};

use crate::prelude::*;
use crate::queries::{Account, CreateAccessTokenResultSuccess, CreatedAccessToken};
use crate::utils::{check_access_token_valid, check_logged_account_id_match};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct AuthMut;

#[common_macros::method_names_consts(const_value_prefix = "GQL: ")]
#[Object]
impl AuthMut {
    #[tracing::instrument(level = "info", name = AuthMut_login, skip_all, fields(%login_method))]
    async fn login(
        &self,
        ctx: &Context<'_>,
        login_method: String,
        login_credentials_json: String,
    ) -> Result<LoginResponse> {
        let authentication_service = from_catalog_n!(ctx, dyn kamu_accounts::AuthenticationService);

        let login_result = authentication_service
            .login(login_method.as_str(), login_credentials_json)
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

    #[tracing::instrument(level = "info", name = AuthMut_create_access_token, skip_all, fields(%account_id))]
    async fn create_access_token(
        &self,
        ctx: &Context<'_>,
        account_id: AccountID<'static>,
        token_name: String,
    ) -> Result<CreateTokenResult> {
        check_logged_account_id_match(ctx, &account_id)?;

        let access_token_service = from_catalog_n!(ctx, dyn kamu_accounts::AccessTokenService);

        match access_token_service
            .create_access_token(&token_name, &account_id)
            .await
        {
            Ok(created_token) => Ok(CreateTokenResult::Success(CreateAccessTokenResultSuccess {
                token: CreatedAccessToken::new(created_token, account_id, &token_name),
            })),
            Err(err) => match err {
                CreateAccessTokenError::Duplicate(_) => Ok(CreateTokenResult::DuplicateName(
                    CreateAccessTokenResultDuplicate { token_name },
                )),
                CreateAccessTokenError::Internal(internal_err) => {
                    Err(GqlError::Internal(internal_err))
                }
            },
        }
    }

    #[tracing::instrument(level = "info", name = AuthMut_revoke_access_token, skip_all)]
    async fn revoke_access_token(
        &self,
        ctx: &Context<'_>,
        token_id: AccessTokenID<'static>,
    ) -> Result<RevokeResult> {
        check_access_token_valid(ctx, &token_id).await?;

        let access_token_service = from_catalog_n!(ctx, dyn kamu_accounts::AccessTokenService);

        match access_token_service.revoke_access_token(&token_id).await {
            Ok(_) => Ok(RevokeResult::Success(RevokeResultSuccess { token_id })),
            Err(RevokeTokenError::AlreadyRevoked) => {
                Ok(RevokeResult::AlreadyRevoked(RevokeResultAlreadyRevoked {
                    token_id,
                }))
            }
            Err(RevokeTokenError::NotFound(_)) => Err(GqlError::Gql(async_graphql::Error::new(
                "Access token not found",
            ))),
            Err(RevokeTokenError::Internal(e)) => Err(e.into()),
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

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RevokeResult {
    Success(RevokeResultSuccess),
    AlreadyRevoked(RevokeResultAlreadyRevoked),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct RevokeResultSuccess {
    pub token_id: AccessTokenID<'static>,
}

#[ComplexObject]
impl RevokeResultSuccess {
    async fn message(&self) -> String {
        "Access token revoked successfully".to_string()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct RevokeResultAlreadyRevoked {
    pub token_id: AccessTokenID<'static>,
}

#[ComplexObject]
impl RevokeResultAlreadyRevoked {
    async fn message(&self) -> String {
        format!("Access token with id {} already revoked", self.token_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateTokenResult {
    Success(CreateAccessTokenResultSuccess),
    DuplicateName(CreateAccessTokenResultDuplicate),
}

#[derive(SimpleObject, Debug)]
#[graphql(complex)]
pub struct CreateAccessTokenResultDuplicate {
    pub token_name: String,
}

#[ComplexObject]
impl CreateAccessTokenResultDuplicate {
    pub async fn message(&self) -> String {
        format!("Access token with {} name already exists", self.token_name)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
