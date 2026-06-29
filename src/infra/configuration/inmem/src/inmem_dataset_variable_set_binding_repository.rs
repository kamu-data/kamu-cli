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
    DatasetVariableSetBindingRepository,
    ReplaceDatasetBindingsError,
};
use kamu_datasets::DatasetEntryRemovalListener;
use kamu_resources::ResourceID;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetVariableSetBindingRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetVariableSetBindingRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetVariableSetBindingRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    resource_ids_by_dataset_id: HashMap<odf::DatasetID, Vec<ResourceID>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetVariableSetBindingRepository for InMemoryDatasetVariableSetBindingRepository {
    async fn list_bindings(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<DatasetConfigurationSetBinding>, InternalError> {
        let guard = self.state.lock().unwrap();

        Ok(guard
            .resource_ids_by_dataset_id
            .get(dataset_id)
            .into_iter()
            .flatten()
            .enumerate()
            .map(
                |(binding_order, resource_id)| DatasetConfigurationSetBinding {
                    dataset_id: dataset_id.clone(),
                    resource_id: *resource_id,
                    binding_order: u64::try_from(binding_order).unwrap(),
                },
            )
            .collect())
    }

    async fn replace_bindings(
        &self,
        dataset_id: &odf::DatasetID,
        resource_ids: &[ResourceID],
    ) -> Result<(), ReplaceDatasetBindingsError> {
        validate_unique_bindings(dataset_id, resource_ids)?;

        let mut guard = self.state.lock().unwrap();
        guard
            .resource_ids_by_dataset_id
            .insert(dataset_id.clone(), resource_ids.to_vec());

        Ok(())
    }

    async fn delete_bindings_for_dataset(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.resource_ids_by_dataset_id.remove(dataset_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetVariableSetBindingRepository {
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
    resource_ids: &[ResourceID],
) -> Result<(), ReplaceDatasetBindingsError> {
    let mut seen = HashSet::new();

    for resource_id in resource_ids {
        if !seen.insert(*resource_id) {
            return Err(DatasetResourceBindingDuplicateError {
                dataset_id: dataset_id.clone(),
                resource_id: *resource_id,
            }
            .into());
        }
    }

    Ok(())
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
