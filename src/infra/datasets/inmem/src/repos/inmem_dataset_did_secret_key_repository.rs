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
use kamu_datasets::{
    DatasetDidSecretKeyRepository,
    GetDatasetDidSecretKeysByOwnerIdError,
    SaveDatasetDidSecretKeyError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    dataset_dids_by_owner_id: HashMap<odf::AccountID, Vec<odf::DatasetID>>,
    did_secret_keys_by_dataset_id: HashMap<odf::DatasetID, DidSecretKey>,
}

impl State {
    fn new() -> Self {
        Self {
            dataset_dids_by_owner_id: HashMap::new(),
            did_secret_keys_by_dataset_id: HashMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetDidSecretKeyRepository {
    state: Arc<Mutex<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DatasetDidSecretKeyRepository)]
#[dill::scope(dill::Singleton)]
impl InMemoryDatasetDidSecretKeyRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetDidSecretKeyRepository for InMemoryDatasetDidSecretKeyRepository {
    async fn save_did_secret_key(
        &self,
        dataset_id: &odf::DatasetID,
        owner_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDatasetDidSecretKeyError> {
        let mut state = self.state.lock().unwrap();
        state
            .did_secret_keys_by_dataset_id
            .insert(dataset_id.clone(), did_secret_key.clone());
        state
            .dataset_dids_by_owner_id
            .entry(owner_id.clone())
            .or_default()
            .push(dataset_id.clone());
        Ok(())
    }

    async fn get_did_secret_keys_by_owner_id(
        &self,
        owner_id: &odf::AccountID,
    ) -> Result<Vec<DidSecretKey>, GetDatasetDidSecretKeysByOwnerIdError> {
        let state = self.state.lock().unwrap();
        let did_secret_key_ids = state
            .dataset_dids_by_owner_id
            .get(owner_id)
            .cloned()
            .unwrap_or_default();
        let did_secret_keys = did_secret_key_ids
            .into_iter()
            .filter_map(|id| state.did_secret_keys_by_dataset_id.get(&id).cloned())
            .collect();
        Ok(did_secret_keys)
    }
}
