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
    DatasetHandlesResolution,
    DatasetRegistry,
    GetMultipleDatasetsError,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct DatasetRegistrySoloUnitBridge {
    dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>,
}

#[component(pub)]
#[interface(dyn DatasetRegistry)]
impl DatasetRegistrySoloUnitBridge {
    pub fn new(dataset_storage_unit: Arc<dyn odf::DatasetStorageUnit>) -> Self {
        Self {
            dataset_storage_unit,
        }
    }
}

#[async_trait::async_trait]
impl odf::dataset::DatasetHandleResolver for DatasetRegistrySoloUnitBridge {
    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, odf::dataset::GetDatasetError> {
        self.dataset_storage_unit
            .resolve_stored_dataset_handle_by_ref(dataset_ref)
            .await
    }
}

#[async_trait::async_trait]
impl DatasetRegistry for DatasetRegistrySoloUnitBridge {
    fn all_dataset_handles(&self) -> odf::dataset::DatasetHandleStream<'_> {
        self.dataset_storage_unit.stored_dataset_handles()
    }

    fn all_dataset_handles_by_owner(
        &self,
        owner_name: &odf::AccountName,
    ) -> odf::dataset::DatasetHandleStream<'_> {
        self.dataset_storage_unit
            .stored_dataset_handles_by_owner(owner_name)
    }

    async fn resolve_multiple_dataset_handles_by_ids(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError> {
        use odf::dataset::DatasetHandleResolver;

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

    fn get_dataset_by_handle(&self, dataset_handle: &odf::DatasetHandle) -> ResolvedDataset {
        let dataset = self
            .dataset_storage_unit
            .get_stored_dataset_by_handle(dataset_handle);
        ResolvedDataset::new(dataset, dataset_handle.clone())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
