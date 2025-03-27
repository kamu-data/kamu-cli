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
use kamu_auth_rebac::RebacDatasetRegistryFacade;
use kamu_core::auth;
use kamu_datasets::{
    DatasetLifecycleMessage,
    NameCollisionError,
    RenameDatasetError,
    RenameDatasetUseCase,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};

use crate::{DatasetEntryWriter, RenameDatasetEntryError};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RenameDatasetUseCaseImpl {
    rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
    dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
    outbox: Arc<dyn Outbox>,
}

#[component(pub)]
#[interface(dyn RenameDatasetUseCase)]
impl RenameDatasetUseCaseImpl {
    pub fn new(
        rebac_dataset_registry_facade: Arc<dyn RebacDatasetRegistryFacade>,
        dataset_entry_writer: Arc<dyn DatasetEntryWriter>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            rebac_dataset_registry_facade,
            dataset_entry_writer,
            outbox,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl RenameDatasetUseCase for RenameDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = RenameDatasetUseCaseImpl_execute,
        skip_all,
        fields(dataset_ref, new_name)
    )]
    async fn execute(
        &self,
        dataset_ref: &odf::DatasetRef,
        new_name: &odf::DatasetName,
    ) -> Result<(), RenameDatasetError> {
        // Locate dataset
        let dataset_handle = self
            .rebac_dataset_registry_facade
            .resolve_dataset_handle_by_ref(dataset_ref, auth::DatasetAction::Maintain)
            .await
            .map_err(|e| {
                use kamu_auth_rebac::RebacDatasetRefUnresolvedError as E;
                match e {
                    E::NotFound(e) => RenameDatasetError::NotFound(e),
                    E::Access(e) => RenameDatasetError::Access(e),
                    e @ E::Internal(_) => RenameDatasetError::Internal(e.int_err()),
                }
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

        // Issue outbox event
        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::renamed(dataset_handle.id.clone(), new_name.clone()),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
