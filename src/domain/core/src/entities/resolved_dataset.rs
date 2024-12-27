// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Clone)]
pub struct ResolvedDataset {
    dataset: Arc<dyn odf::Dataset>,
    handle: odf::DatasetHandle,
}

impl ResolvedDataset {
    pub fn new(dataset: Arc<dyn odf::Dataset>, handle: odf::DatasetHandle) -> Self {
        Self { dataset, handle }
    }

    pub fn from(create_dataset_result: &odf::CreateDatasetResult) -> Self {
        Self {
            dataset: create_dataset_result.dataset.clone(),
            handle: create_dataset_result.dataset_handle.clone(),
        }
    }

    #[inline]
    pub fn get_id(&self) -> &odf::DatasetID {
        &self.handle.id
    }

    #[inline]
    pub fn get_alias(&self) -> &odf::DatasetAlias {
        &self.handle.alias
    }

    #[inline]
    pub fn get_handle(&self) -> &odf::DatasetHandle {
        &self.handle
    }

    #[inline]
    pub fn take_handle(self) -> odf::DatasetHandle {
        self.handle
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::ops::Deref for ResolvedDataset {
    type Target = Arc<dyn odf::Dataset>;
    fn deref(&self) -> &Self::Target {
        &self.dataset
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

impl std::fmt::Debug for ResolvedDataset {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.handle.fmt(f)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
