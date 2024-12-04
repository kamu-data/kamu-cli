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

use dill::*;
use internal_error::InternalError;
use kamu_datasets::{
    BlockPointer,
    DatasetEntryRemovalListener,
    DatasetReferenceNotFoundError,
    DatasetReferenceRepository,
    GetDatasetReferenceError,
};
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    references: HashMap<odf::DatasetID, HashMap<String, BlockPointer>>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetReferenceRepository {
    state: Arc<Mutex<State>>,
}

#[component(pub)]
#[interface(dyn DatasetReferenceRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
#[scope(Singleton)]
impl InMemoryDatasetReferenceRepository {
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(State::default())),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetReferenceRepository for InMemoryDatasetReferenceRepository {
    async fn set_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
        block_ptr: BlockPointer,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get_mut(dataset_id) {
            if let Some(ptr_ref) = dataset_references.get_mut(block_ref_name) {
                *ptr_ref = block_ptr;
            } else {
                dataset_references.insert(block_ref_name.to_string(), block_ptr);
            }
        } else {
            guard.references.insert(
                dataset_id.clone(),
                HashMap::from_iter([(block_ref_name.to_string(), block_ptr)]),
            );
        }

        Ok(())
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
    ) -> Result<BlockPointer, GetDatasetReferenceError> {
        let guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get(dataset_id) {
            if let Some(block_ptr) = dataset_references.get(block_ref_name) {
                return Ok(block_ptr.clone());
            }
        }

        Err(GetDatasetReferenceError::NotFound(
            DatasetReferenceNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref_name: block_ref_name.to_string(),
            },
        ))
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetReferenceRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        let mut guard = self.state.lock().unwrap();
        guard.references.remove(dataset_id);
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
