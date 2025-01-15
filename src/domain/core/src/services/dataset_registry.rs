// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric::{AccountName, DatasetHandle, DatasetID, DatasetRef};
use thiserror::Error;

use crate::{DatasetHandleStream, GetDatasetError, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetRegistry: Send + Sync {
    fn all_dataset_handles(&self) -> DatasetHandleStream<'_>;

    fn all_dataset_handles_by_owner(&self, owner_name: &AccountName) -> DatasetHandleStream<'_>;

    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<DatasetHandle, GetDatasetError>;

    async fn resolve_multiple_dataset_handles_by_ids(
        &self,
        dataset_ids: Vec<DatasetID>,
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError>;

    fn get_dataset_by_handle(&self, dataset_handle: &DatasetHandle) -> ResolvedDataset;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetRegistryExt: DatasetRegistry {
    async fn try_resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<DatasetHandle>, InternalError>;

    async fn get_dataset_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<ResolvedDataset, GetDatasetError>;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl<T> DatasetRegistryExt for T
where
    T: DatasetRegistry,
    T: ?Sized,
{
    async fn try_resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<Option<DatasetHandle>, InternalError> {
        match self.resolve_dataset_handle_by_ref(dataset_ref).await {
            Ok(hdl) => Ok(Some(hdl)),
            Err(GetDatasetError::NotFound(_)) => Ok(None),
            Err(GetDatasetError::Internal(e)) => Err(e),
        }
    }

    async fn get_dataset_by_ref(
        &self,
        dataset_ref: &DatasetRef,
    ) -> Result<ResolvedDataset, GetDatasetError> {
        let dataset_handle = self.resolve_dataset_handle_by_ref(dataset_ref).await?;
        let dataset = self.get_dataset_by_handle(&dataset_handle);
        Ok(dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct DatasetHandlesResolution {
    pub resolved_handles: Vec<DatasetHandle>,
    pub unresolved_datasets: Vec<(DatasetID, GetDatasetError)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetMultipleDatasetsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
