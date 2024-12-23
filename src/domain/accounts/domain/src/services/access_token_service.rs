// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use database_common::PaginationOpts;
use uuid::Uuid;

use crate::{
    AccessToken,
    Account,
    CreateAccessTokenError,
    FindAccountByTokenError,
    GetAccessTokenError,
    KamuAccessToken,
    RevokeTokenError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const DUMMY_ACCESS_TOKEN: &str = "some-token";

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait AccessTokenService: Sync + Send {
    async fn create_access_token(
        &self,
        token_name: &str,
        account_id: &odf::AccountID,
    ) -> Result<KamuAccessToken, CreateAccessTokenError>;

    async fn find_account_by_active_token_id(
        &self,
        token_id: &Uuid,
        token_hash: [u8; 32],
    ) -> Result<Account, FindAccountByTokenError>;

    async fn get_token_by_id(&self, token_id: &Uuid) -> Result<AccessToken, GetAccessTokenError>;

    async fn get_access_tokens_by_account_id(
        &self,
        account_id: &odf::AccountID,
        pagination: &PaginationOpts,
    ) -> Result<AccessTokenListing, GetAccessTokenError>;

    async fn revoke_access_token(&self, token_id: &Uuid) -> Result<(), RevokeTokenError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccessTokenListing {
    pub list: Vec<AccessToken>,
    pub total_count: usize,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
