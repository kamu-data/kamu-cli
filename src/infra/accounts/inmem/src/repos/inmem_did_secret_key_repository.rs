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

use kamu_accounts::{
    DidEntity,
    DidEntityType,
    DidSecretKey,
    DidSecretKeyRepository,
    GetDidSecretKeysByCreatorIdError,
    SaveDidSecretKeyError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    dids_by_creator_id: HashMap<odf::AccountID, Vec<DidEntity<'static>>>,
    did_secret_keys_by_entity: HashMap<DidEntity<'static>, DidSecretKey>,
}

impl State {
    fn new() -> Self {
        Self {
            dids_by_creator_id: HashMap::new(),
            did_secret_keys_by_entity: HashMap::new(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDidSecretKeyRepository {
    state: Arc<Mutex<State>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn DidSecretKeyRepository)]
#[dill::scope(dill::Singleton)]
impl InMemoryDidSecretKeyRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::new())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DidSecretKeyRepository for InMemoryDidSecretKeyRepository {
    async fn save_did_secret_key(
        &self,
        entity: &DidEntity,
        creator_id: &odf::AccountID,
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDidSecretKeyError> {
        let mut state = self.state.lock().unwrap();
        state
            .did_secret_keys_by_entity
            .insert(entity.clone().into_owned(), did_secret_key.clone());
        state
            .dids_by_creator_id
            .entry(creator_id.clone())
            .or_default()
            .push(entity.clone().into_owned());
        Ok(())
    }

    async fn get_did_secret_keys_by_creator_id(
        &self,
        creator_id: &odf::AccountID,
        entity_type_maybe: Option<DidEntityType>,
    ) -> Result<Vec<DidSecretKey>, GetDidSecretKeysByCreatorIdError> {
        let state = self.state.lock().unwrap();
        let did_secret_key_entities = state
            .dids_by_creator_id
            .get(creator_id)
            .cloned()
            .unwrap_or_default();
        let did_secret_keys = did_secret_key_entities
            .into_iter()
            .filter_map(|entity| {
                if let Some(entity_type) = entity_type_maybe
                    && entity_type != entity.entity_type
                {
                    return None;
                }
                state.did_secret_keys_by_entity.get(&entity).cloned()
            })
            .collect();
        Ok(did_secret_keys)
    }
}
