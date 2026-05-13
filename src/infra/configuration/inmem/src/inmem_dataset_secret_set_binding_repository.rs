// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use dill::{Singleton, component, interface, scope};
use internal_error::InternalError;
use kamu_configuration::{
    DatasetConfigurationSetBinding,
    DatasetResourceBindingDuplicateError,
    DatasetSecretSetBindingRepository,
    ReplaceDatasetBindingsError,
};
use kamu_datasets::DatasetEntryRemovalListener;
use kamu_resources::ResourceUID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetSecretSetBindingRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetSecretSetBindingRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetSecretSetBindingRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    resource_uids_by_dataset_id: HashMap<odf::DatasetID, Vec<ResourceUID>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetSecretSetBindingRepository for InMemoryDatasetSecretSetBindingRepository {
    async fn replace_bindings(
        &self,
        dataset_id: &odf::DatasetID,
        resource_uids: &[ResourceUID],
    ) -> Result<(), ReplaceDatasetBindingsError> {
        validate_unique_bindings(dataset_id, resource_uids)?;

        let mut guard = self.state.lock().unwrap();
        guard
            .resource_uids_by_dataset_id
            .insert(dataset_id.clone(), resource_uids.to_vec());

        Ok(())
    }

    async fn list_bindings(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<DatasetConfigurationSetBinding>, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .resource_uids_by_dataset_id
            .get(dataset_id)
            .into_iter()
            .flatten()
            .enumerate()
            .map(
                |(binding_order, resource_uid)| DatasetConfigurationSetBinding {
                    dataset_id: dataset_id.clone(),
                    resource_uid: *resource_uid,
                    binding_order: u64::try_from(binding_order).unwrap(),
                },
            )
            .collect())
    }

    async fn delete_bindings_for_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.resource_uids_by_dataset_id.remove(dataset_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetSecretSetBindingRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        self.delete_bindings_for_dataset(dataset_id).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

fn validate_unique_bindings(
    dataset_id: &odf::DatasetID,
    resource_uids: &[ResourceUID],
) -> Result<(), ReplaceDatasetBindingsError> {
    let mut seen = HashSet::new();

    for resource_uid in resource_uids {
        if !seen.insert(*resource_uid) {
            return Err(DatasetResourceBindingDuplicateError {
                dataset_id: dataset_id.clone(),
                resource_uid: *resource_uid,
            }
            .into());
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
