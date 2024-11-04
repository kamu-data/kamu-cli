// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;
use std::sync::Arc;

use opendatafabric::{DatasetHandle, DatasetID};

use crate::Dataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct WorkingDatasetsMap {
    datasets_by_id: HashMap<DatasetID, Arc<dyn Dataset>>,
    handles_by_id: HashMap<DatasetID, DatasetHandle>,
}

impl WorkingDatasetsMap {
    pub fn get_by_id(&self, id: &DatasetID) -> Arc<dyn Dataset> {
        self.datasets_by_id
            .get(id)
            .expect("Dataset must be present")
            .clone()
    }

    #[inline]
    pub fn get_by_handle(&self, handle: &DatasetHandle) -> Arc<dyn Dataset> {
        self.get_by_id(&handle.id)
    }

    pub fn get_handle_by_id(&self, id: &DatasetID) -> &DatasetHandle {
        self.handles_by_id.get(id).expect("Dataset must be present")
    }

    pub fn register(&mut self, handle: &DatasetHandle, dataset: Arc<dyn Dataset>) {
        if !self.datasets_by_id.contains_key(&handle.id) {
            self.datasets_by_id.insert(handle.id.clone(), dataset);
            self.handles_by_id.insert(handle.id.clone(), handle.clone());
        }
    }

    pub fn register_with(
        &mut self,
        handle: &DatasetHandle,
        dataset_fn: impl Fn(&DatasetHandle) -> Arc<dyn Dataset>,
    ) {
        if !self.datasets_by_id.contains_key(&handle.id) {
            let dataset = dataset_fn(handle);
            self.datasets_by_id.insert(handle.id.clone(), dataset);
            self.handles_by_id.insert(handle.id.clone(), handle.clone());
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
