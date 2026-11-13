// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use cheap_clone::CheapClone;
use database_common::{TransactionId, TransactionRef};
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

use super::InMemoryDatasetReferenceState;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct InMemoryDatasetReferenceRepository {
    state: Arc<InMemoryDatasetReferenceState>,
    transaction_id: Option<TransactionId>,
}

#[component(pub)]
#[interface(dyn DatasetReferenceRepository)]
#[interface(dyn DatasetEntryRemovalListener)]
impl InMemoryDatasetReferenceRepository {
    pub fn new(
        state: Arc<InMemoryDatasetReferenceState>,
        transaction_ref: Option<TransactionRef>,
    ) -> Self {
        Self {
            state,
            transaction_id: transaction_ref.map(|transaction_ref| transaction_ref.id()),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetReferenceRepository for InMemoryDatasetReferenceRepository {
    async fn has_any_references(&self) -> Result<bool, InternalError> {
        Ok(self
            .state
            .references
            .read(self.transaction_id, |references| !references.is_empty()))
    }

    async fn get_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<odf::Multihash, GetDatasetReferenceError> {
        self.state
            .references
            .read(self.transaction_id, |references| {
                references
                    .get(&(dataset_id.clone(), block_ref.cheap_clone()))
                    .cloned()
            })
            .ok_or_else(|| {
                GetDatasetReferenceError::NotFound(DatasetReferenceNotFoundError {
                    dataset_id: dataset_id.clone(),
                    block_ref: block_ref.cheap_clone(),
                })
            })
    }

    async fn get_all_dataset_references(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<Vec<(odf::BlockRef, odf::Multihash)>, InternalError> {
        Ok(self
            .state
            .references
            .read(self.transaction_id, |references| {
                references
                    .iter()
                    .filter(|((id, _), _)| id == dataset_id)
                    .map(|((_, block_ref), block_hash)| {
                        (block_ref.cheap_clone(), block_hash.clone())
                    })
                    .collect()
            }))
    }

    async fn set_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        block_hash: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError> {
        let key = (dataset_id.clone(), block_ref.cheap_clone());
        self.state
            .references
            .write(self.transaction_id, |references| {
                let view = references.view();
                let current_block_hash = view.get(&key);
                if current_block_hash != maybe_prev_block_hash {
                    return Err(DatasetReferenceCASError::new(
                        dataset_id,
                        block_ref,
                        maybe_prev_block_hash,
                        current_block_hash,
                    )
                    .into());
                }

                references.put(key, block_hash.clone());
                Ok(())
            })
    }

    async fn remove_dataset_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<(), RemoveDatasetReferenceError> {
        let key = (dataset_id.clone(), block_ref.cheap_clone());
        self.state
            .references
            .write(self.transaction_id, |references| {
                if !references.view().contains_key(&key) {
                    return Err(RemoveDatasetReferenceError::NotFound(
                        DatasetReferenceNotFoundError {
                            dataset_id: dataset_id.clone(),
                            block_ref: block_ref.cheap_clone(),
                        },
                    ));
                }

                references.delete(key);
                Ok(())
            })
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl DatasetEntryRemovalListener for InMemoryDatasetReferenceRepository {
    async fn on_dataset_entry_removed(
        &self,
        dataset_id: &odf::DatasetID,
    ) -> Result<(), InternalError> {
        self.state
            .references
            .write(self.transaction_id, |references| {
                references.delete_where(|(id, _)| id == dataset_id);
            });
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
