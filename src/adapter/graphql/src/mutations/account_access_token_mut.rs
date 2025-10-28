// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use kamu_accounts::{Account, CreateAccessTokenError, RevokeTokenError};

use crate::prelude::*;
use crate::queries::{CreateAccessTokenResultSuccess, CreatedAccessToken};
use crate::utils::check_access_token_valid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccountAccessTokensMut<'a> {
    account: &'a Account,
}

#[common_macros::method_names_consts(const_value_prefix = "Gql::")]
#[Object]
impl<'a> AccountAccessTokensMut<'a> {
    #[graphql(skip)]
    pub fn new(account: &'a Account) -> Self {
        Self { account }
    }

    #[tracing::instrument(level = "info", name = AccountAccessTokensMut_create_access_token, skip_all)]
    async fn create_access_token(
        &self,
        ctx: &Context<'_>,
        token_name: String,
    ) -> Result<CreateTokenResult<'_>> {
        let access_token_service = from_catalog_n!(ctx, dyn kamu_accounts::AccessTokenService);

        match access_token_service
            .create_access_token(&token_name, &self.account.id.clone())
            .await
        {
            Ok(created_token) => Ok(CreateTokenResult::Success(Box::new(
                CreateAccessTokenResultSuccess {
                    token: CreatedAccessToken::new(
                        created_token,
                        self.account.id.clone().into(),
                        &token_name,
                    ),
                },
            ))),
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

    #[tracing::instrument(level = "info", name = AccountAccessTokensMut_revoke_access_token, skip_all)]
    async fn revoke_access_token(
        &self,
        ctx: &Context<'_>,
        token_id: AccessTokenID<'static>,
    ) -> Result<RevokeResult<'_>> {
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

#[derive(Interface)]
#[graphql(field(name = "message", ty = "String"))]
pub enum RevokeResult<'a> {
    Success(RevokeResultSuccess<'a>),
    AlreadyRevoked(RevokeResultAlreadyRevoked<'a>),
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct RevokeResultSuccess<'a> {
    pub token_id: AccessTokenID<'a>,
}

#[ComplexObject]
impl RevokeResultSuccess<'_> {
    async fn message(&self) -> String {
        "Access token revoked successfully".to_string()
    }
}

#[derive(SimpleObject)]
#[graphql(complex)]
pub struct RevokeResultAlreadyRevoked<'a> {
    pub token_id: AccessTokenID<'a>,
}

#[ComplexObject]
impl RevokeResultAlreadyRevoked<'_> {
    async fn message(&self) -> String {
        format!("Access token with id {} already revoked", self.token_id)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Interface, Debug)]
#[graphql(field(name = "message", ty = "String"))]
pub enum CreateTokenResult<'a> {
    Success(Box<CreateAccessTokenResultSuccess<'a>>),
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
