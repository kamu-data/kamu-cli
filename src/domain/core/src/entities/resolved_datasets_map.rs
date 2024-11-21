// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use opendatafabric::{DatasetHandle, DatasetID};

use crate::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct ResolvedDatasetsMap {
    resolved_datasets_by_id: HashMap<DatasetID, ResolvedDataset>,
}

impl ResolvedDatasetsMap {
    pub fn get_by_id(&self, id: &DatasetID) -> &ResolvedDataset {
        self.resolved_datasets_by_id
            .get(id)
            .expect("Dataset must be present")
    }

    #[inline]
    pub fn get_by_handle(&self, handle: &DatasetHandle) -> &ResolvedDataset {
        self.get_by_id(&handle.id)
    }

    pub fn iterate_all_handles(&self) -> impl Iterator<Item = &DatasetHandle> {
        self.resolved_datasets_by_id
            .values()
            .map(ResolvedDataset::get_handle)
    }

    pub fn register(&mut self, resolved_dataset: ResolvedDataset) {
        if !self
            .resolved_datasets_by_id
            .contains_key(resolved_dataset.get_id())
        {
            self.resolved_datasets_by_id
                .insert(resolved_dataset.get_id().clone(), resolved_dataset);
        }
    }

    pub fn register_with(
        &mut self,
        handle: &DatasetHandle,
        dataset_fn: impl Fn(&DatasetHandle) -> ResolvedDataset,
    ) {
        if !self.resolved_datasets_by_id.contains_key(&handle.id) {
            let resolved_dataset = dataset_fn(handle);
            self.resolved_datasets_by_id
                .insert(handle.id.clone(), resolved_dataset);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
