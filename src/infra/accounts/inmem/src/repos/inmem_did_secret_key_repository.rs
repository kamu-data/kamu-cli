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

use kamu_accounts::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    did_secret_keys_by_entity: HashMap<DidEntity<'static>, DidSecretKey>,
}

impl State {
    fn new() -> Self {
        Self {
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
        did_secret_key: &DidSecretKey,
    ) -> Result<(), SaveDidSecretKeyError> {
        let mut state = self.state.lock().unwrap();

        state
            .did_secret_keys_by_entity
            .insert(entity.clone().into_owned(), did_secret_key.clone());

        Ok(())
    }

    async fn get_did_secret_key(
        &self,
        entity: &DidEntity,
    ) -> Result<DidSecretKey, GetDidSecretKeyError> {
        let state = self.state.lock().unwrap();

        let entity = entity.clone().into_owned();

        if let Some(did_secret_key) = state.did_secret_keys_by_entity.get(&entity) {
            Ok(did_secret_key.clone())
        } else {
            Err(GetDidSecretKeyError::NotFound(entity.into()))
        }
    }

    async fn delete_did_secret_key(
        &self,
        entity: &DidEntity,
    ) -> Result<(), DeleteDidSecretKeyError> {
        let mut state = self.state.lock().unwrap();

        let entity = entity.clone().into_owned();
        let has_deleted = state.did_secret_keys_by_entity.remove(&entity).is_some();

        if has_deleted {
            Ok(())
        } else {
            Err(DeleteDidSecretKeyError::NotFound(entity.into()))
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
