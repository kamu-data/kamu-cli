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
use kamu_accounts::CurrentAccountSubject;
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer, DatasetActionUnauthorizedError};
use kamu_core::{DatasetRegistry, DatasetStorageUnitWriter};
use kamu_datasets::{
    DatasetLifecycleMessage,
    RenameDatasetUseCase,
    MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

pub struct RenameDatasetUseCaseImpl {
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_storage_unit_writer: Arc<dyn DatasetStorageUnitWriter>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    outbox: Arc<dyn Outbox>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

#[component(pub)]
#[interface(dyn RenameDatasetUseCase)]
impl RenameDatasetUseCaseImpl {
    pub fn new(
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_storage_unit_writer: Arc<dyn DatasetStorageUnitWriter>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        outbox: Arc<dyn Outbox>,
        current_account_subject: Arc<CurrentAccountSubject>,
    ) -> Self {
        Self {
            dataset_registry,
            dataset_storage_unit_writer,
            dataset_action_authorizer,
            outbox,
            current_account_subject,
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
        let owner_account_id = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account cannot rename dataset");
            }
            CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        };
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

        let old_name = dataset_handle.alias.dataset_name.clone();

        self.dataset_storage_unit_writer
            .rename_dataset(&dataset_handle, new_name)
            .await?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_DATASET_SERVICE,
                DatasetLifecycleMessage::renamed(
                    dataset_handle.id.clone(),
                    owner_account_id,
                    old_name,
                    new_name.clone(),
                ),
            )
            .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
