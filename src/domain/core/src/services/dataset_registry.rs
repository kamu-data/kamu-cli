// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use thiserror::Error;

use crate::ResolvedDataset;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetRegistry: odf::dataset::DatasetHandleResolver {
    fn all_dataset_handles(&self) -> odf::dataset::DatasetHandleStream<'_>;

    fn all_dataset_handles_by_owner(
        &self,
        owner_name: &odf::AccountName,
    ) -> odf::dataset::DatasetHandleStream<'_>;

    async fn resolve_multiple_dataset_handles_by_ids(
        &self,
        dataset_ids: Vec<odf::DatasetID>,
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError>;

    fn get_dataset_by_handle(&self, dataset_handle: &odf::DatasetHandle) -> ResolvedDataset;
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Extensions
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetRegistryExt: DatasetRegistry {
    async fn try_resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<odf::DatasetHandle>, InternalError>;

    async fn get_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<ResolvedDataset, odf::dataset::GetDatasetError>;
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
        dataset_ref: &odf::DatasetRef,
    ) -> Result<Option<odf::DatasetHandle>, InternalError> {
        match self.resolve_dataset_handle_by_ref(dataset_ref).await {
            Ok(hdl) => Ok(Some(hdl)),
            Err(odf::dataset::GetDatasetError::NotFound(_)) => Ok(None),
            Err(odf::dataset::GetDatasetError::Internal(e)) => Err(e),
        }
    }

    async fn get_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<ResolvedDataset, odf::dataset::GetDatasetError> {
        let dataset_handle = self.resolve_dataset_handle_by_ref(dataset_ref).await?;
        let dataset = self.get_dataset_by_handle(&dataset_handle);
        Ok(dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct DatasetHandlesResolution {
    pub resolved_handles: Vec<odf::DatasetHandle>,
    pub unresolved_datasets: Vec<(odf::DatasetID, odf::dataset::GetDatasetError)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetMultipleDatasetsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
