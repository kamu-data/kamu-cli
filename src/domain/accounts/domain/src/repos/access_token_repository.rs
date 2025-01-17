// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use database_common::PaginationOpts;
use internal_error::InternalError;
use thiserror::Error;
use uuid::Uuid;

use crate::{AccessToken, Account};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccessTokenRepository: Send + Sync {
    async fn save_access_token(
        &self,
        access_token: &AccessToken,
    ) -> Result<(), CreateAccessTokenError>;

    async fn get_token_by_id(&self, token_id: &Uuid) -> Result<AccessToken, GetAccessTokenError>;

    async fn get_access_tokens_by_account_id(
        &self,
        account_id: &odf::AccountID,
        pagination: &PaginationOpts,
    ) -> Result<Vec<AccessToken>, GetAccessTokenError>;

    async fn get_access_tokens_count_by_account_id(
        &self,
        account_id: &odf::AccountID,
    ) -> Result<usize, GetAccessTokenError>;

    async fn mark_revoked(
        &self,
        token_id: &Uuid,
        revoke_time: DateTime<Utc>,
    ) -> Result<(), RevokeTokenError>;

    async fn find_account_by_active_token_id(
        &self,
        token_id: &Uuid,
        token_hash: [u8; 32],
    ) -> Result<Account, FindAccountByTokenError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CreateAccessTokenError {
    #[error(transparent)]
    Duplicate(CreateAccessTokenErrorDuplicate),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Access token not created, duplicate {access_token_name}")]
pub struct CreateAccessTokenErrorDuplicate {
    pub access_token_name: String,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum GetAccessTokenError {
    #[error(transparent)]
    NotFound(AccessTokenNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

#[derive(Error, Debug)]
#[error("Access token not found: '{access_token_id}'")]
pub struct AccessTokenNotFoundError {
    pub access_token_id: Uuid,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum FindAccountByTokenError {
    #[error("Access token hash is invalid")]
    InvalidTokenHash,

    #[error(transparent)]
    NotFound(AccessTokenNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum RevokeTokenError {
    #[error("Access token already revoked")]
    AlreadyRevoked,

    #[error(transparent)]
    NotFound(AccessTokenNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
