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
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::{DatasetRepository, GetDatasetError, RenameDatasetError, RenameDatasetUseCase};
use opendatafabric::{DatasetName, DatasetRef};

use crate::DatasetRepositoryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RenameDatasetUseCaseImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

#[component(pub)]
#[interface(dyn RenameDatasetUseCase)]
impl RenameDatasetUseCaseImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_repo_writer,
            dataset_action_authorizer,
        }
    }
}

#[async_trait::async_trait]
impl RenameDatasetUseCase for RenameDatasetUseCaseImpl {
    async fn execute(
        &self,
        dataset_ref: &DatasetRef,
        new_name: &DatasetName,
    ) -> Result<(), RenameDatasetError> {
        let dataset_handle = match self.dataset_repo.resolve_dataset_ref(dataset_ref).await {
            Ok(h) => Ok(h),
            Err(GetDatasetError::NotFound(e)) => Err(RenameDatasetError::NotFound(e)),
            Err(GetDatasetError::Internal(e)) => Err(RenameDatasetError::Internal(e)),
        }?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Write)
            .await?;

        self.dataset_repo_writer
            .rename_dataset(dataset_ref, new_name)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
