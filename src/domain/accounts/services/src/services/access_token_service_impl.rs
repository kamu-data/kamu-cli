// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::PaginationOpts;
use dill::*;
use internal_error::InternalError;
use kamu_accounts::{
    AccessToken,
    AccessTokenLifecycleMessage,
    AccessTokenListing,
    AccessTokenRepository,
    AccessTokenService,
    Account,
    CreateAccessTokenError,
    FindAccountByTokenError,
    GetAccessTokenError,
    KamuAccessToken,
    MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE,
    RevokeTokenError,
};
use messaging_outbox::{Outbox, OutboxExt};
use time_source::SystemTimeSource;
use uuid::Uuid;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub const ACCESS_TOKEN_PREFIX: &str = "ka";
pub const ACCESS_TOKEN_BYTES_LENGTH: usize = 16;
pub const ENCODED_ACCESS_TOKEN_LENGTH: usize = 61;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AccessTokenServiceImpl {
    access_token_repository: Arc<dyn AccessTokenRepository>,
    time_source: Arc<dyn SystemTimeSource>,
    outbox: Arc<dyn Outbox>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn AccessTokenService)]
impl AccessTokenServiceImpl {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(
        access_token_repository: Arc<dyn AccessTokenRepository>,
        time_source: Arc<dyn SystemTimeSource>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            access_token_repository,
            time_source,
            outbox,
        }
    }

    async fn notify_access_token_created(
        &self,
        token_name: String,
        account_id: odf::AccountID,
    ) -> Result<(), InternalError> {
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_ACCESS_TOKEN_SERVICE,
                AccessTokenLifecycleMessage::created(token_name, account_id),
            )
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccessTokenService for AccessTokenServiceImpl {
    async fn create_access_token(
        &self,
        token_name: &str,
        account_id: &odf::AccountID,
    ) -> Result<KamuAccessToken, CreateAccessTokenError> {
        let kamu_access_token = KamuAccessToken::new();

        self.access_token_repository
            .save_access_token(&AccessToken {
                id: kamu_access_token.id,
                token_name: token_name.to_string(),
                token_hash: kamu_access_token.random_bytes_hash,
                created_at: self.time_source.now(),
                revoked_at: None,
                account_id: account_id.clone(),
            })
            .await?;

        self.notify_access_token_created(token_name.to_owned(), account_id.clone())
            .await?;

        Ok(kamu_access_token)
    }

    async fn find_account_by_active_token_id(
        &self,
        token_id: &Uuid,
        token_hash: [u8; 32],
    ) -> Result<Account, FindAccountByTokenError> {
        self.access_token_repository
            .find_account_by_active_token_id(token_id, token_hash)
            .await
    }

    async fn get_access_tokens_by_account_id(
        &self,
        account_id: &odf::AccountID,
        pagination: &PaginationOpts,
    ) -> Result<AccessTokenListing, GetAccessTokenError> {
        let total_count = self
            .access_token_repository
            .get_access_tokens_count_by_account_id(account_id)
            .await?;
        if total_count == 0 {
            return Ok(AccessTokenListing {
                total_count,
                list: vec![],
            });
        }

        let access_token_list = self
            .access_token_repository
            .get_access_tokens_by_account_id(account_id, pagination)
            .await?;
        Ok(AccessTokenListing {
            total_count,
            list: access_token_list,
        })
    }

    async fn revoke_access_token(&self, token_id: &Uuid) -> Result<(), RevokeTokenError> {
        self.access_token_repository
            .mark_revoked(token_id, self.time_source.now())
            .await
    }

    async fn get_token_by_id(&self, token_id: &Uuid) -> Result<AccessToken, GetAccessTokenError> {
        self.access_token_repository.get_token_by_id(token_id).await
    }
}
