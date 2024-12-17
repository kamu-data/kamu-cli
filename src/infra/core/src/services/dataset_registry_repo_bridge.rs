// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::*;
use kamu_core::{
    DatasetHandleStream,
    DatasetHandlesResolution,
    DatasetRegistry,
    DatasetRepository,
    GetDatasetError,
    GetMultipleDatasetsError,
    ResolvedDataset,
};
use opendatafabric::{AccountName, DatasetHandle, DatasetID, DatasetRef};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetRegistryRepoBridge {
    dataset_repo: Arc<dyn DatasetRepository>,
}

#[component(pub)]
#[interface(dyn DatasetRegistry)]
impl DatasetRegistryRepoBridge {
    pub fn new(dataset_repo: Arc<dyn DatasetRepository>) -> Self {
        Self { dataset_repo }
    }
}

#[async_trait::async_trait]
impl DatasetRegistry for DatasetRegistryRepoBridge {
    fn all_dataset_handles(&self) -> DatasetHandleStream<'_> {
        self.dataset_repo.all_dataset_handles()
    }

    fn all_dataset_handles_by_owner(&self, owner_name: &AccountName) -> DatasetHandleStream<'_> {
        self.dataset_repo.all_dataset_handles_by_owner(owner_name)
    }

    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError> {
        self.dataset_repo
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await
    }

    async fn resolve_multiple_dataset_handles_by_ids(
        &self,
        dataset_ids: Vec<DatasetID>,
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError> {
        let mut res: DatasetHandlesResolution = Default::default();

        for dataset_id in dataset_ids {
            let dataset_ref = dataset_id.as_local_ref();
            let resolve_res = self.resolve_dataset_handle_by_ref(&dataset_ref).await;
            match resolve_res {
                Ok(hdl) => res.resolved_handles.push(hdl),
                Err(e) => res.unresolved_datasets.push((dataset_id, e)),
            }
        }

        Ok(res)
    }

    fn get_dataset_by_handle(&self, dataset_handle: &DatasetHandle) -> ResolvedDataset {
        let dataset = self.dataset_repo.get_dataset_by_handle(dataset_handle);
        ResolvedDataset::new(dataset, dataset_handle.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
