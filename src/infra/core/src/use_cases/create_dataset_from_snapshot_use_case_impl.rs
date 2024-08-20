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
use internal_error::ResultIntoInternal;
use kamu_accounts::CurrentAccountSubject;
use kamu_auth_rebac::{DatasetPropertyName, RebacService};
use kamu_core::{
    CreateDatasetFromSnapshotError,
    CreateDatasetFromSnapshotResult,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetFromSnapshotUseCaseOptions,
    CreateDatasetResult,
    DatasetLifecycleMessage,
    DatasetRepository,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};
use opendatafabric::DatasetSnapshot;

use crate::DatasetRepositoryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CreateDatasetFromSnapshotUseCase)]
pub struct CreateDatasetFromSnapshotUseCaseImpl {
    current_account_subject: Arc<CurrentAccountSubject>,
    dataset_repo_reader: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    rebac_service: Arc<dyn RebacService>,
    outbox: Arc<dyn Outbox>,
}

impl CreateDatasetFromSnapshotUseCaseImpl {
    pub fn new(
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_repo_reader: Arc<dyn DatasetRepository>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        rebac_service: Arc<dyn RebacService>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            current_account_subject,
            dataset_repo_reader,
            dataset_repo_writer,
            rebac_service,
            outbox,
        }
    }

    async fn handle_multi_tenant_case(
        &self,
        create_dataset_result: &CreateDatasetResult,
        options: &CreateDatasetFromSnapshotUseCaseOptions,
    ) -> Result<(), CreateDatasetFromSnapshotError> {
        // Trying to set the ReBAC property
        let allows = options.dataset_visibility.is_publicly_available();
        let (name, value) = DatasetPropertyName::allows_public_read(allows);

        let set_rebac_property_res = self
            .rebac_service
            .set_dataset_property(&create_dataset_result.dataset_handle.id, name, &value)
            .await
            .map_int_err(CreateDatasetFromSnapshotError::Internal);

        // If setting fails, try to clean up the created dataset
        if let Err(set_rebac_property_err) = set_rebac_property_res {
            self.dataset_repo_writer
                .delete_dataset(&create_dataset_result.dataset_handle)
                .await
                // Return a deletion error if there is one
                .map_int_err(CreateDatasetFromSnapshotError::Internal)?;

            // On successful deletion, return the property setting error
            return Err(set_rebac_property_err);
        }

        // Dataset created and property set
        Ok(())
    }
}

#[async_trait::async_trait]
impl CreateDatasetFromSnapshotUseCase for CreateDatasetFromSnapshotUseCaseImpl {
    async fn execute(
        &self,
        snapshot: DatasetSnapshot,
        options: &CreateDatasetFromSnapshotUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        let CreateDatasetFromSnapshotResult {
            create_dataset_result,
            new_upstream_ids,
        } = self
            .dataset_repo_writer
            .create_dataset_from_snapshot(snapshot)
            .await?;

        let is_multi_tenant_workspace = self.dataset_repo_reader.is_multi_tenant();

        if is_multi_tenant_workspace {
            self.handle_multi_tenant_case(&create_dataset_result, options)
                .await?;
        }

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    create_dataset_result.dataset_handle.id.clone(),
                    match self.current_account_subject.as_ref() {
                        CurrentAccountSubject::Anonymous(_) => {
                            panic!("Anonymous account cannot create dataset");
                        }
                        CurrentAccountSubject::Logged(l) => l.account_id.clone(),
                    },
                ),
            )
            .await?;

        if !new_upstream_ids.is_empty() {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                    DatasetLifecycleMessage::dependencies_updated(
                        create_dataset_result.dataset_handle.id.clone(),
                        new_upstream_ids,
                    ),
                )
                .await?;
        }

        Ok(create_dataset_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
