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
use kamu_datasets::{
    DatasetReferenceCASError,
    DatasetReferenceNotFoundError,
    DatasetReferenceService,
    GetDatasetReferenceError,
    SetDatasetReferenceError,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Keeps dataset references in memory, without storage or messages
pub struct FakeDatasetReferenceService {
    references: Arc<Mutex<HashMap<(odf::DatasetID, odf::BlockRef), odf::Multihash>>>,
}

#[component(pub)]
#[interface(dyn DatasetReferenceService)]
#[scope(Singleton)]
impl FakeDatasetReferenceService {
    pub fn new() -> Self {
        Self {
            references: Default::default(),
        }
    }

    /// Points the head of the dataset at `head`
    pub fn set_head(&self, dataset_id: &odf::DatasetID, head: &odf::Multihash) {
        self.references
            .lock()
            .unwrap()
            .insert((dataset_id.clone(), odf::BlockRef::Head), head.clone());
    }
}

#[async_trait::async_trait]
impl DatasetReferenceService for FakeDatasetReferenceService {
    async fn get_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
    ) -> Result<odf::Multihash, GetDatasetReferenceError> {
        self.references
            .lock()
            .unwrap()
            .get(&(dataset_id.clone(), block_ref.clone()))
            .cloned()
            .ok_or_else(|| {
                DatasetReferenceNotFoundError {
                    dataset_id: dataset_id.clone(),
                    block_ref: block_ref.clone(),
                }
                .into()
            })
    }

    async fn set_reference(
        &self,
        dataset_id: &odf::DatasetID,
        block_ref: &odf::BlockRef,
        maybe_prev_block_hash: Option<&odf::Multihash>,
        new_block_hash: &odf::Multihash,
    ) -> Result<(), SetDatasetReferenceError> {
        let mut references = self.references.lock().unwrap();
        let key = (dataset_id.clone(), block_ref.clone());

        let actual_prev_block_hash = references.get(&key);
        if actual_prev_block_hash != maybe_prev_block_hash {
            return Err(DatasetReferenceCASError {
                dataset_id: dataset_id.clone(),
                block_ref: block_ref.clone(),
                expected_prev_block_hash: maybe_prev_block_hash.cloned(),
                actual_prev_block_hash: actual_prev_block_hash.cloned(),
            }
            .into());
        }

        references.insert(key, new_block_hash.clone());
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
