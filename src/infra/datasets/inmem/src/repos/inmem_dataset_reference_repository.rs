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
    DatasetEntryRemovalListener,
    DatasetReferenceCASError,
    DatasetReferenceNotFoundError,
    DatasetReferenceRepository,
    GetDatasetReferenceError,
    SetDatasetReferenceError,
};
use opendatafabric as odf;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    references: HashMap<odf::DatasetID, HashMap<String, odf::Multihash>>,
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
        maybe_prev_block_hash: Option<&odf::Multihash>,
        block_hash: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError> {
        let mut guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get_mut(dataset_id) {
            if let Some(hash_ref) = dataset_references.get_mut(block_ref_name) {
                match maybe_prev_block_hash {
                    Some(expected_prev_block_hash) => {
                        if expected_prev_block_hash != hash_ref {
                            return Err(DatasetReferenceCASError::new(
                                dataset_id,
                                block_ref_name,
                                maybe_prev_block_hash,
                                Some(hash_ref),
                            )
                            .into());
                        }

                        *hash_ref = block_hash.clone();
                    }
                    None => {
                        return Err(DatasetReferenceCASError::new(
                            dataset_id,
                            block_ref_name,
                            None,
                            Some(hash_ref),
                        )
                        .into());
                    }
                }
            } else {
                if maybe_prev_block_hash.is_some() {
                    return Err(DatasetReferenceCASError::new(
                        dataset_id,
                        block_ref_name,
                        maybe_prev_block_hash,
                        None,
                    )
                    .into());
                }

                dataset_references.insert(block_ref_name.to_string(), block_hash.clone());
            }
        } else {
            if maybe_prev_block_hash.is_some() {
                return Err(DatasetReferenceCASError::new(
                    dataset_id,
                    block_ref_name,
                    maybe_prev_block_hash,
                    None,
                )
                .into());
            }

            guard.references.insert(
                dataset_id.clone(),
                HashMap::from_iter([(block_ref_name.to_string(), block_hash.clone())]),
            );
        }

        Ok(())
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref_name: &str,
    ) -> Result<odf::Multihash, GetDatasetReferenceError> {
        let guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get(dataset_id) {
            if let Some(block_hash) = dataset_references.get(block_ref_name) {
                return Ok(block_hash.clone());
            }
        }

        Err(GetDatasetReferenceError::NotFound(
            DatasetReferenceNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref_name: block_ref_name.to_string(),
            },
        ))
    }

    async fn get_all_dataset_references(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<(String, odf::Multihash)>, InternalError> {
        let guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get(dataset_id) {
            Ok(dataset_references
                .iter()
                .map(|(block_ref_name, block_hash)| (block_ref_name.clone(), block_hash.clone()))
                .collect())
        } else {
            Ok(vec![])
        }
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