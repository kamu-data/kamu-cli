// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use crate::domain::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    nonce_by_wallet: HashMap<EvmWalletAddress, Web3AuthEip4361NonceEntity>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryWeb3AuthNonceRepository {
    state: Arc<RwLock<State>>,
}

#[dill::component(pub)]
#[dill::interface(dyn Web3AuthNonceRepository)]
#[dill::scope(dill::Singleton)]
impl InMemoryWeb3AuthNonceRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Web3AuthEip4361NonceRepository for InMemoryWeb3AuthNonceRepository {
    async fn set_nonce(&self, entity: &Web3AuthEip4361NonceEntity) -> Result<(), SetNonceError> {
        let mut writable_state = self.state.write().await;

        writable_state
            .nonce_by_wallet
            .insert(entity.wallet_address, (*entity).clone());

        Ok(())
    }

    async fn get_nonce(
        &self,
        wallet: &EvmWalletAddress,
    ) -> Result<Web3AuthEip4361NonceEntity, GetNonceError> {
        let readable_state = self.state.read().await;

        if let Some(nonce_entity) = readable_state.nonce_by_wallet.get(wallet) {
            Ok(nonce_entity.clone())
        } else {
            Err(GetNonceError::NotFound { wallet: *wallet })
        }
    }

    async fn cleanup_expired_nonces(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CleanupExpiredNoncesError> {
        let mut writable_state = self.state.write().await;

        writable_state
            .nonce_by_wallet
            .retain(|_, nonce_entity| nonce_entity.expires_at > now);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
