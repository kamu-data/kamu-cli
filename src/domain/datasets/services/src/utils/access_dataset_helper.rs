// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use internal_error::{ErrorIntoInternal, InternalError};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::DatasetRegistry;
use kamu_datasets::{
    EditDatasetUseCaseError,
    EditMultiResponse,
    ViewDatasetUseCaseError,
    ViewMultiResponse,
};
use odf::dataset::{DatasetNotFoundError, GetDatasetError};
use thiserror::Error;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub(crate) struct AccessDatasetHelper<'a> {
    dataset_registry: &'a Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: &'a Arc<dyn DatasetActionAuthorizer>,
}

impl<'a> AccessDatasetHelper<'a> {
    pub fn new(
        dataset_registry: &'a Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: &'a Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_action_authorizer,
        }
    }

    pub async fn access_dataset(
        &self,
        dataset_ref: &odf::DatasetRef,
        action: DatasetAction,
    ) -> Result<odf::DatasetHandle, DatasetAccessError> {
        let handle = match self
            .dataset_registry
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await
        {
            Ok(handle) => Ok(handle),
            Err(e) => match e {
                GetDatasetError::NotFound(e) => Err(DatasetAccessError::NotFound(e)),
                unexpected_error => Err(unexpected_error.int_err().into()),
            },
        }?;

        self.dataset_action_authorizer
            .check_action_allowed(&handle.id, action)
            .await
            .map_err(|e| match e {
                DatasetActionUnauthorizedError::Access(e) => DatasetAccessError::Access(e),
                unexpected_error => DatasetAccessError::Internal(unexpected_error.int_err()),
            })?;

        Ok(handle)
    }

    pub async fn access_multi_dataset(
        &self,
        dataset_refs: Vec<odf::DatasetRef>,
        action: DatasetAction,
    ) -> Result<AccessMultiDatasetResponse, InternalError> {
        let mut multi_result = AccessMultiDatasetResponse {
            viewable_resolved_refs: Vec::with_capacity(dataset_refs.len()),
            inaccessible_refs: vec![],
        };

        // TODO: Private Datasets: resolve multi refs at once
        for dataset_ref in dataset_refs {
            match self.access_dataset(&dataset_ref, action).await {
                Ok(handle) => {
                    multi_result
                        .viewable_resolved_refs
                        .push((dataset_ref, handle));
                }
                Err(e) => match e {
                    e @ (DatasetAccessError::NotFound(_) | DatasetAccessError::Access(_)) => {
                        multi_result.inaccessible_refs.push((dataset_ref, e));
                    }
                    DatasetAccessError::Internal(e) => return Err(e),
                },
            }
        }

        Ok(multi_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Debug)]
pub(crate) struct AccessMultiDatasetResponse {
    pub viewable_resolved_refs: Vec<(odf::DatasetRef, odf::DatasetHandle)>,
    pub inaccessible_refs: Vec<(odf::DatasetRef, DatasetAccessError)>,
}

impl From<AccessMultiDatasetResponse> for ViewMultiResponse {
    fn from(v: AccessMultiDatasetResponse) -> Self {
        Self {
            viewable_resolved_refs: v.viewable_resolved_refs,
            inaccessible_refs: v
                .inaccessible_refs
                .into_iter()
                .map(|(dr, e)| (dr, e.into()))
                .collect(),
        }
    }
}

impl From<AccessMultiDatasetResponse> for EditMultiResponse {
    fn from(v: AccessMultiDatasetResponse) -> Self {
        Self {
            viewable_resolved_refs: v.viewable_resolved_refs,
            inaccessible_refs: v
                .inaccessible_refs
                .into_iter()
                .map(|(dr, e)| (dr, e.into()))
                .collect(),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[derive(Error, Debug)]
pub(crate) enum DatasetAccessError {
    #[error(transparent)]
    NotFound(#[from] DatasetNotFoundError),

    #[error(transparent)]
    Access(
        #[from]
        #[backtrace]
        odf::AccessError,
    ),

    #[error(transparent)]
    Internal(
        #[from]
        #[backtrace]
        InternalError,
    ),
}

impl From<DatasetAccessError> for ViewDatasetUseCaseError {
    fn from(value: DatasetAccessError) -> Self {
        match value {
            DatasetAccessError::NotFound(e) => Self::NotFound(e),
            DatasetAccessError::Access(e) => Self::Access(e),
            DatasetAccessError::Internal(e) => Self::Internal(e),
        }
    }
}

impl From<DatasetAccessError> for EditDatasetUseCaseError {
    fn from(value: DatasetAccessError) -> Self {
        match value {
            DatasetAccessError::NotFound(e) => Self::NotFound(e),
            DatasetAccessError::Access(e) => Self::Access(e),
            DatasetAccessError::Internal(e) => Self::Internal(e),
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
