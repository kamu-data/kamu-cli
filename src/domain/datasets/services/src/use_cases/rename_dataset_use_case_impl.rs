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
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::{DatasetRegistry, DatasetStorageUnitWriter};
use kamu_datasets::RenameDatasetUseCase;

use crate::DatasetEntryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RenameDatasetUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    dataset_storage_unit_writer: Arc<dyn DatasetStorageUnitWriter>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

#[component(pub)]
#[interface(dyn RenameDatasetUseCase)]
impl RenameDatasetUseCaseImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
        dataset_storage_unit_writer: Arc<dyn DatasetStorageUnitWriter>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_entry_writer,
            dataset_storage_unit_writer,
            dataset_action_authorizer,
        }
    }
}

#[async_trait::async_trait]
impl RenameDatasetUseCase for RenameDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "RenameDatasetUseCase::execute",
        skip_all,
        fields(dataset_ref, new_name)
    )]
    async fn execute(
        &self,
        dataset_ref: &odf::DatasetRef,
        new_name: &odf::DatasetName,
    ) -> Result<(), odf::dataset::RenameDatasetError> {
        let dataset_handle = match self
            .dataset_registry
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await
        {
            Ok(h) => Ok(h),
            Err(odf::dataset::GetDatasetError::NotFound(e)) => {
                Err(odf::dataset::RenameDatasetError::NotFound(e))
            }
            Err(odf::dataset::GetDatasetError::Internal(e)) => {
                Err(odf::dataset::RenameDatasetError::Internal(e))
            }
        }?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle.id, DatasetAction::Write)
            .await
            .map_err(|e| match e {
                DatasetActionUnauthorizedError::Access(e) => {
                    odf::dataset::RenameDatasetError::Access(e)
                }
                DatasetActionUnauthorizedError::Internal(e) => {
                    odf::dataset::RenameDatasetError::Internal(e)
                }
            })?;

        // TODO: error handling such as name collision should move to this level.
        self.dataset_entry_writer
            .rename_entry(&dataset_handle, new_name)
            .await?;

        // TODO: once we get rid of aliases and unify repo storage,
        // there will be nothing to do at storage level on rename
        self.dataset_storage_unit_writer
            .rename_dataset(&dataset_handle, new_name)
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
