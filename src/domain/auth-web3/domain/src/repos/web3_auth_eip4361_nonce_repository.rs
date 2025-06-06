// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use chrono::{DateTime, Utc};
use internal_error::InternalError;
use thiserror::Error;

use crate::{EvmWalletAddress, Web3AuthEip4361NonceEntity};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait Web3AuthEip4361NonceRepository: Send + Sync {
    async fn set_nonce(&self, entity: &Web3AuthEip4361NonceEntity) -> Result<(), SetNonceError>;

    async fn get_nonce(
        &self,
        wallet: &EvmWalletAddress,
    ) -> Result<Web3AuthEip4361NonceEntity, GetNonceError>;

    async fn consume_nonce(
        &self,
        wallet: &EvmWalletAddress,
        now: DateTime<Utc>,
    ) -> Result<(), ConsumeNonceError>;

    async fn cleanup_expired_nonces(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CleanupExpiredNoncesError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum SetNonceError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for SetNonceError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug, PartialEq)]
#[error("nonce not found for wallet: {wallet}")]
pub struct NonceNotFoundError {
    pub wallet: EvmWalletAddress,
}

#[derive(Error, Debug)]
pub enum GetNonceError {
    #[error(transparent)]
    NotFound(#[from] NonceNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for GetNonceError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NotFound(a), Self::NotFound(b)) => a == b,
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum ConsumeNonceError {
    #[error(transparent)]
    NotFound(#[from] NonceNotFoundError),

    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for ConsumeNonceError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::NotFound(a), Self::NotFound(b)) => a == b,
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
            (_, _) => false,
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub enum CleanupExpiredNoncesError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

impl PartialEq for CleanupExpiredNoncesError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Internal(a), Self::Internal(b)) => a.reason().eq(&b.reason()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
