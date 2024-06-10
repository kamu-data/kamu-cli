// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

///////////////////////////////////////////////////////////////////////////////

use opendatafabric::AccountID;
use thiserror::Error;

use crate::{
    AccessToken,
    AccessTokenPaginationOpts,
    Account,
    CreateAccessTokenError,
    GetAccessTokenError,
    GetAccountInfoError,
    KamuAccessToken,
};

#[async_trait::async_trait]
pub trait AccessTokenService: Sync + Send {
    async fn create_access_token(
        &self,
        token_name: &str,
        account_id: &AccountID,
    ) -> Result<KamuAccessToken, CreateAccessTokenError>;

    fn encode_access_token(
        &self,
        access_token_str: &str,
    ) -> Result<KamuAccessToken, EncodeTokenError>;

    fn generate_access_token(&self) -> KamuAccessToken;

    async fn find_account_by_access_token(
        &self,
        access_token: String,
    ) -> Result<Account, GetAccountInfoError>;

    async fn get_access_tokens(
        &self,
        pagination: &AccessTokenPaginationOpts,
    ) -> Result<Vec<AccessToken>, GetAccessTokenError>;
}

///////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug, Eq, PartialEq)]
pub enum EncodeTokenError {
    #[error("Invalid access token prefix")]
    InvalidTokenPrefix,
    #[error("Invalid access token length")]
    InvalidTokenLength,
    #[error("Invalid access token checksum")]
    InvalidTokenChecksum,
    #[error("Invalid access token format")]
    InvalidTokenFormat,
}
