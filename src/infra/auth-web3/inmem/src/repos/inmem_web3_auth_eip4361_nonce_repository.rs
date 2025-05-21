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

pub struct InMemoryWeb3AuthEip4361NonceRepository {
    state: Arc<RwLock<State>>,
}

#[dill::component(pub)]
#[dill::interface(dyn Web3AuthEip4361NonceRepository)]
#[dill::scope(dill::Singleton)]
impl InMemoryWeb3AuthEip4361NonceRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl Web3AuthEip4361NonceRepository for InMemoryWeb3AuthEip4361NonceRepository {
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
            Err(GetNonceError::NotFound(NonceNotFoundError {
                wallet: *wallet,
            }))
        }
    }

    async fn consume_nonce(
        &self,
        wallet: &EvmWalletAddress,
        now: DateTime<Utc>,
    ) -> Result<(), ConsumeNonceError> {
        use std::collections::hash_map::Entry;

        let mut writable_state = self.state.write().await;

        match writable_state.nonce_by_wallet.entry(*wallet) {
            Entry::Occupied(entry) if now <= entry.get().expires_at => {
                entry.remove();
                Ok(())
            }
            _ => Err(ConsumeNonceError::NotFound(NonceNotFoundError {
                wallet: *wallet,
            })),
        }
    }

    async fn cleanup_expired_nonces(
        &self,
        now: DateTime<Utc>,
    ) -> Result<(), CleanupExpiredNoncesError> {
        let mut writable_state = self.state.write().await;

        writable_state
            .nonce_by_wallet
            .retain(|_, nonce_entity| now <= nonce_entity.expires_at);

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
