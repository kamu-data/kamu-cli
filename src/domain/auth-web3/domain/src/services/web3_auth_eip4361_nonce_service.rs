// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;

use crate::{ConsumeNonceError, EvmWalletAddress, Web3AuthEip4361NonceEntity};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait Web3AuthEip4361NonceService: Send + Sync {
    async fn create_nonce(
        &self,
        wallet_address: EvmWalletAddress,
    ) -> Result<Web3AuthEip4361NonceEntity, CreateNonceError>;

    async fn consume_nonce(
        &self,
        wallet_address: &EvmWalletAddress,
    ) -> Result<(), ConsumeNonceError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(thiserror::Error, Debug)]
pub enum CreateNonceError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
