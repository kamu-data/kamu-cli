// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use opendatafabric::DatasetHandle;

use super::Dataset;
use crate::CreateDatasetResult;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ResolvedDataset {
    pub dataset: Arc<dyn Dataset>,
    pub handle: DatasetHandle,
}

impl ResolvedDataset {
    pub fn new(dataset: Arc<dyn Dataset>, handle: DatasetHandle) -> Self {
        Self { dataset, handle }
    }

    pub fn from(create_dataset_result: &CreateDatasetResult) -> Self {
        Self {
            dataset: create_dataset_result.dataset.clone(),
            handle: create_dataset_result.dataset_handle.clone(),
        }
    }
}

impl std::fmt::Debug for ResolvedDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.handle.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
