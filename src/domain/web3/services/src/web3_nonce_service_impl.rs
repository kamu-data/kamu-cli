// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::ResultIntoInternal;
use kamu_web3::{
    CreateNonceError,
    EvmWalletAddress,
    Web3AuthNonceRepository,
    Web3AuthenticationNonce,
    Web3AuthenticationNonceEntity,
    Web3NonceService,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn Web3NonceService)]
pub struct Web3NonceServiceImpl {
    nonce_repo: Arc<dyn Web3AuthNonceRepository>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Web3NonceService for Web3NonceServiceImpl {
    async fn create_nonce(
        &self,
        wallet_address: EvmWalletAddress,
    ) -> Result<Web3AuthenticationNonceEntity, CreateNonceError> {
        let entity = Web3AuthenticationNonceEntity {
            wallet_address,
            nonce: Web3AuthenticationNonce::new(),
            expired_at: Default::default(),
        };

        self.nonce_repo.set_nonce(&entity).await.int_err()?;

        Ok(entity)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
