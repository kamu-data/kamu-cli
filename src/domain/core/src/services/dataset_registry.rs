// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::InternalError;
use opendatafabric as odf;
use thiserror::Error;

use crate::{DatasetHandleStream, GetDatasetError, ResolvedDataset};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
pub trait DatasetRegistry: Send + Sync {
    fn all_dataset_handles(&self) -> DatasetHandleStream<'_>;

    // TODO: Private Datasets: replace AccountName with AccountID?
    fn all_dataset_handles_by_owner(
        &self,
        owner_name: &odf::AccountName,
    ) -> DatasetHandleStream<'_>;

    /// Datasets to which the user can potentially have access
    fn all_potentially_related_dataset_handles_by_owner(
        &self,
        owner_name: &odf::AccountName,
    ) -> DatasetHandleStream<'_>;

    async fn resolve_dataset_handle_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, GetDatasetError>;

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
    ) -> Result<ResolvedDataset, GetDatasetError>;

    fn all_owned_and_potentially_related_dataset_handles(
        &self,
        owner_name: &odf::AccountName,
    ) -> AllOwnedAndPotentiallyRelatedDatasetHandlesResult;
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
            Err(GetDatasetError::NotFound(_)) => Ok(None),
            Err(GetDatasetError::Internal(e)) => Err(e),
        }
    }

    async fn get_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<ResolvedDataset, GetDatasetError> {
        let dataset_handle = self.resolve_dataset_handle_by_ref(dataset_ref).await?;
        let dataset = self.get_dataset_by_handle(&dataset_handle);
        Ok(dataset)
    }

    // TODO: Private Datasets: use AccountID?
    fn all_owned_and_potentially_related_dataset_handles(
        &self,
        owner_name: &odf::AccountName,
    ) -> AllOwnedAndPotentiallyRelatedDatasetHandlesResult {
        AllOwnedAndPotentiallyRelatedDatasetHandlesResult {
            owned_handles_stream: self.all_dataset_handles_by_owner(owner_name),
            potentially_related_handles_stream: self
                .all_potentially_related_dataset_handles_by_owner(owner_name),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct DatasetHandlesResolution {
    pub resolved_handles: Vec<odf::DatasetHandle>,
    pub unresolved_datasets: Vec<(odf::DatasetID, GetDatasetError)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct AllOwnedAndPotentiallyRelatedDatasetHandlesResult<'a> {
    pub owned_handles_stream: DatasetHandleStream<'a>,
    pub potentially_related_handles_stream: DatasetHandleStream<'a>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Errors
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetMultipleDatasetsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
