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
    RemoveDatasetReferenceError,
    SetDatasetReferenceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
struct State {
    references: HashMap<odf::DatasetID, HashMap<odf::BlockRef, odf::Multihash>>,
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
    async fn has_any_references(&self) -> Result<bool, InternalError> {
        let guard = self.state.lock().unwrap();
        Ok(!guard.references.is_empty())
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<odf::Multihash, GetDatasetReferenceError> {
        let guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get(dataset_id)
            && let Some(block_hash) = dataset_references.get(block_ref)
        {
            return Ok(block_hash.clone());
        }

        Err(GetDatasetReferenceError::NotFound(
            DatasetReferenceNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref: *block_ref,
            },
        ))
    }

    async fn get_all_dataset_references(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<(odf::BlockRef, odf::Multihash)>, InternalError> {
        let guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get(dataset_id) {
            Ok(dataset_references
                .iter()
                .map(|(block_ref, block_hash)| (*block_ref, block_hash.clone()))
                .collect())
        } else {
            Ok(vec![])
        }
    }

    async fn set_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        block_hash: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError> {
        let mut guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get_mut(dataset_id) {
            if let Some(hash_ref) = dataset_references.get_mut(block_ref) {
                match maybe_prev_block_hash {
                    Some(expected_prev_block_hash) => {
                        if expected_prev_block_hash != hash_ref {
                            return Err(DatasetReferenceCASError::new(
                                dataset_id,
                                block_ref,
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
                            block_ref,
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
                        block_ref,
                        maybe_prev_block_hash,
                        None,
                    )
                    .into());
                }

                dataset_references.insert(*block_ref, block_hash.clone());
            }
        } else {
            if maybe_prev_block_hash.is_some() {
                return Err(DatasetReferenceCASError::new(
                    dataset_id,
                    block_ref,
                    maybe_prev_block_hash,
                    None,
                )
                .into());
            }

            guard.references.insert(
                dataset_id.clone(),
                HashMap::from_iter([(*block_ref, block_hash.clone())]),
            );
        }

        Ok(())
    }

    async fn remove_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), RemoveDatasetReferenceError> {
        let mut guard = self.state.lock().unwrap();
        if let Some(dataset_references) = guard.references.get_mut(dataset_id)
            && dataset_references.remove(block_ref).is_some()
        {
            return Ok(());
        }

        Err(RemoveDatasetReferenceError::NotFound(
            DatasetReferenceNotFoundError {
                dataset_id: dataset_id.clone(),
                block_ref: *block_ref,
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
