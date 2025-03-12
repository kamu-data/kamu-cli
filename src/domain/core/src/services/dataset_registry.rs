// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use internal_error::*;
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
        dataset_ids: &[odf::DatasetID],
    ) -> Result<DatasetHandlesResolution, GetMultipleDatasetsError>;

    async fn get_dataset_by_handle(&self, dataset_handle: &odf::DatasetHandle) -> ResolvedDataset;
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

    /// Attempts to resolve datasets by IDs and returns results in the matching
    /// order (useful for ziping)
    async fn try_resolve_multiple_dataset_handles_by_ids_stable(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<Vec<Result<odf::DatasetHandle, odf::DatasetNotFoundError>>, InternalError>;

    async fn get_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<ResolvedDataset, odf::DatasetRefUnresolvedError>;
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
            Err(odf::DatasetRefUnresolvedError::NotFound(_)) => Ok(None),
            Err(odf::DatasetRefUnresolvedError::Internal(e)) => Err(e),
        }
    }

    async fn try_resolve_multiple_dataset_handles_by_ids_stable(
        &self,
        dataset_ids: &[odf::DatasetID],
    ) -> Result<Vec<Result<odf::DatasetHandle, odf::DatasetNotFoundError>>, InternalError> {
        // TODO: Avoid cloning!
        let resolution = self
            .resolve_multiple_dataset_handles_by_ids(dataset_ids)
            .await
            .int_err()?;

        let mut handles_by_id: std::collections::BTreeMap<_, _> = resolution
            .resolved_handles
            .into_iter()
            .map(|h| (h.id.clone(), h))
            .collect();

        let mut res = Vec::new();
        for id in dataset_ids {
            match handles_by_id.remove(id) {
                Some(h) => res.push(Ok(h)),
                None => res.push(Err(odf::DatasetNotFoundError {
                    dataset_ref: id.as_local_ref(),
                })),
            }
        }

        Ok(res)
    }

    async fn get_dataset_by_ref(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<ResolvedDataset, odf::DatasetRefUnresolvedError> {
        let dataset_handle = self.resolve_dataset_handle_by_ref(dataset_ref).await?;
        let dataset = self.get_dataset_by_handle(&dataset_handle).await;
        Ok(dataset)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Default)]
pub struct DatasetHandlesResolution {
    pub resolved_handles: Vec<odf::DatasetHandle>,
    pub unresolved_datasets: Vec<(odf::DatasetID, odf::DatasetRefUnresolvedError)>,
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug, Error)]
pub enum GetMultipleDatasetsError {
    #[error(transparent)]
    Internal(#[from] InternalError),
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
