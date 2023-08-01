// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use opendatafabric as odf;

use crate::mutations::DatasetMetadataMut;
use crate::prelude::*;

#[derive(Debug, Clone)]
pub struct DatasetMut {
    dataset_handle: odf::DatasetHandle,
}

#[Object]
impl DatasetMut {
    #[graphql(skip)]
    pub fn new(dataset_handle: odf::DatasetHandle) -> Self {
        Self { dataset_handle }
    }

    /// Access to the mutable metadata of the dataset
    async fn metadata(&self) -> DatasetMetadataMut {
        DatasetMetadataMut::new(self.dataset_handle.clone())
    }
}
