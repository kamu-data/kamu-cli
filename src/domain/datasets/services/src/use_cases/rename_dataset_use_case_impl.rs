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
use kamu_core::DatasetRegistry;
use kamu_datasets::{NameCollisionError, RenameDatasetError, RenameDatasetUseCase};

use crate::{DatasetEntryWriter, RenameDatasetEntryError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RenameDatasetUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

#[component(pub)]
#[interface(dyn RenameDatasetUseCase)]
impl RenameDatasetUseCaseImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_entry_writer,
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
    ) -> Result<(), RenameDatasetError> {
        // Locate dataset
        let dataset_handle = match self
            .dataset_registry
            .resolve_dataset_handle_by_ref(dataset_ref)
            .await
        {
            Ok(h) => Ok(h),
            Err(odf::DatasetRefUnresolvedError::NotFound(e)) => {
                Err(RenameDatasetError::NotFound(e))
            }
            Err(odf::DatasetRefUnresolvedError::Internal(e)) => {
                Err(RenameDatasetError::Internal(e))
            }
        }?;

        // Ensure write permissions
        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle.id, DatasetAction::Write)
            .await
            .map_err(|e| match e {
                DatasetActionUnauthorizedError::Access(e) => RenameDatasetError::Access(e),
                DatasetActionUnauthorizedError::Internal(e) => RenameDatasetError::Internal(e),
            })?;

        // Rename entry in the entry repository
        self.dataset_entry_writer
            .rename_entry(&dataset_handle, new_name)
            .await
            .map_err(|e| match e {
                RenameDatasetEntryError::Internal(e) => RenameDatasetError::Internal(e),
                RenameDatasetEntryError::NameCollision(e) => {
                    RenameDatasetError::NameCollision(NameCollisionError {
                        alias: odf::DatasetAlias::new(
                            dataset_handle.alias.account_name.clone(),
                            e.dataset_name,
                        ),
                    })
                }
            })?;

        // Resolve target dataset
        let target = self
            .dataset_registry
            .get_dataset_by_handle(&dataset_handle)
            .await;

        // Update stored alias file
        // TODO: reconsider if we need this in general
        // TODO: consider writing this change only after DB transaction succeeds
        odf::dataset::write_dataset_alias(
            target.as_ref(),
            &odf::DatasetAlias::new(dataset_handle.alias.account_name.clone(), new_name.clone()),
        )
        .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
