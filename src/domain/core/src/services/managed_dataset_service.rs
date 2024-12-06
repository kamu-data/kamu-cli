// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use dill::*;
use internal_error::InternalError;
use opendatafabric as odf;

use crate::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait ManagedDatasetService: Send + Sync {
    async fn new_managed(
        &self,
        new_unmanaged_dataset: ResolvedDataset,
        head: odf::Multihash,
    ) -> Result<ResolvedDataset, InternalError>;

    async fn existing_managed(
        &self,
        unmanaged_dataset: ResolvedDataset,
    ) -> Result<ResolvedDataset, InternalError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ManagedDatasetService)]
pub struct DummyManagedDatasetService {}

#[async_trait::async_trait]
impl ManagedDatasetService for DummyManagedDatasetService {
    async fn new_managed(
        &self,
        new_unmanaged_dataset: ResolvedDataset,
        _head: odf::Multihash,
    ) -> Result<ResolvedDataset, InternalError> {
        Ok(new_unmanaged_dataset)
    }

    async fn existing_managed(
        &self,
        unmanaged_dataset: ResolvedDataset,
    ) -> Result<ResolvedDataset, InternalError> {
        Ok(unmanaged_dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
