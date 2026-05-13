// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use dill::{Singleton, component, interface, scope};
use internal_error::InternalError;
use kamu_configuration::{
    ReplaceProjectionEntriesError,
    SecretSetEntry,
    SecretSetProjectionRepository,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemorySecretSetProjectionRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn SecretSetProjectionRepository)]
#[scope(Singleton)]
impl InMemorySecretSetProjectionRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    entries_by_resource_uid_generation:
        HashMap<(kamu_resources::ResourceUID, u64), Vec<SecretSetEntry>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl SecretSetProjectionRepository for InMemorySecretSetProjectionRepository {
    async fn replace_entries(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
        entries: &[SecretSetEntry],
    ) -> Result<(), ReplaceProjectionEntriesError> {
        let mut guard = self.state.lock().unwrap();
        let key = (*resource_uid, resource_generation);
        if guard.entries_by_resource_uid_generation.contains_key(&key) {
            return Err(ReplaceProjectionEntriesError::concurrent_modification());
        }
        guard
            .entries_by_resource_uid_generation
            .insert(key, entries.to_vec());
        Ok(())
    }

    async fn find_entry(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
        key: &str,
    ) -> Result<Option<SecretSetEntry>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .entries_by_resource_uid_generation
            .get(&(*resource_uid, resource_generation))
            .and_then(|entries| entries.iter().find(|entry| entry.key == key))
            .cloned())
    }

    async fn get_entries(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .entries_by_resource_uid_generation
            .get(&(*resource_uid, resource_generation))
            .cloned()
            .unwrap_or_default())
    }

    async fn get_latest_entries_before_generation(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
    ) -> Result<Vec<SecretSetEntry>, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(guard
            .entries_by_resource_uid_generation
            .iter()
            .filter(|((stored_resource_uid, stored_generation), _)| {
                stored_resource_uid == resource_uid && *stored_generation < resource_generation
            })
            .max_by_key(|((_, stored_generation), _)| *stored_generation)
            .map(|(_, entries)| entries.clone())
            .unwrap_or_default())
    }

    async fn cleanup_entries_before_generation(
        &self,
        resource_uid: &kamu_resources::ResourceUID,
        resource_generation: u64,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.entries_by_resource_uid_generation.retain(
            |(stored_resource_uid, stored_generation), _| {
                stored_resource_uid != resource_uid || *stored_generation >= resource_generation
            },
        );
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
