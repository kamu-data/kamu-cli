// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use dill::{component, interface};
use internal_error::ErrorIntoInternal;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::{DatasetRegistry, ViewDatasetUseCase, ViewDatasetUseCaseError};
use odf::dataset::GetDatasetError;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ViewDatasetUseCase)]
pub struct ViewDatasetUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl ViewDatasetUseCaseImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_action_authorizer,
        }
    }
}

#[async_trait::async_trait]
impl ViewDatasetUseCase for ViewDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "ViewDatasetUseCase::execute",
        skip_all,
        fields(dataset_ref)
    )]
    async fn execute(
        &self,
        dataset_ref: &odf::DatasetRef,
    ) -> Result<odf::DatasetHandle, ViewDatasetUseCaseError> {
        let handle = match self
            .dataset_registry
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await
        {
            Ok(handle) => Ok(handle),
            Err(get_dataset_error) => match get_dataset_error {
                GetDatasetError::NotFound(e) => {
                    Err(ViewDatasetUseCaseError::NotAccessible(e.into()))
                }
                unexpected_error => Err(unexpected_error.int_err().into()),
            },
        }?;

        self.dataset_action_authorizer
            .check_action_allowed(&handle.id, DatasetAction::Read)
            .await
            .map_err(|e| match e {
                access_error @ DatasetActionUnauthorizedError::Access(_) => {
                    ViewDatasetUseCaseError::NotAccessible(access_error.into())
                }
                unexpected_error => ViewDatasetUseCaseError::Internal(unexpected_error.int_err()),
            })?;

        Ok(handle)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
