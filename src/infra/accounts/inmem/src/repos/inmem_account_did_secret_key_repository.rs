// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crypto_utils::DidSecretKey;
use kamu_accounts::{
    AccountDidSecretKeyRepository,
    GetDidSecretKeysByAccountIdError,
    SaveAccountDidSecretKeyError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    account_dids_by_creator_id: HashMap<odf::AccountID, Vec<odf::AccountID>>,
    did_secret_keys_by_account_id: HashMap<odf::AccountID, DidSecretKey>,
}

impl State {
    fn new() -> Self {
        Self {
            account_dids_by_creator_id: HashMap::new(),
            did_secret_keys_by_account_id: HashMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryAccountDidSecretKeyRepository {
    state: Arc<Mutex<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn AccountDidSecretKeyRepository)]
#[dill::scope(dill::Singleton)]
impl InMemoryAccountDidSecretKeyRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl AccountDidSecretKeyRepository for InMemoryAccountDidSecretKeyRepository {
    async fn save_did_secret_key(
        &self,
        account_id: &odf::AccountID,
        creator_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveAccountDidSecretKeyError> {
        let mut state = self.state.lock().unwrap();
        state
            .did_secret_keys_by_account_id
            .insert(account_id.clone(), did_secret_key.clone());
        state
            .account_dids_by_creator_id
            .entry(creator_id.clone())
            .or_default()
            .push(account_id.clone());
        Ok(())
    }

    async fn get_did_secret_keys_by_creator_id(
        &self,
        creator_id: &odf::AccountID,
    ) -> Result<Vec<DidSecretKey>, GetDidSecretKeysByAccountIdError> {
        let state = self.state.lock().unwrap();
        let did_secret_key_ids = state
            .account_dids_by_creator_id
            .get(creator_id)
            .cloned()
            .unwrap_or_default();
        let did_secret_keys = did_secret_key_ids
            .into_iter()
            .filter_map(|id| state.did_secret_keys_by_account_id.get(&id).cloned())
            .collect();
        Ok(did_secret_keys)
    }
}
