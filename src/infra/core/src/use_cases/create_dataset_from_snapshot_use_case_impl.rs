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
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    rebac_service: Arc<dyn RebacService>,
    outbox: Arc<dyn Outbox>,
}

impl CreateDatasetFromSnapshotUseCaseImpl {
    pub fn new(
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        rebac_service: Arc<dyn RebacService>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            current_account_subject,
            dataset_repo_writer,
            rebac_service,
            outbox,
        }
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

        let is_multi_tenant_workspace = self.dataset_repo_writer.is_multi_tenant();
        // TODO: Test fail scenario -- do we need clean-up in that case?
        let created_dataset_id = &create_dataset_result.dataset_handle.id;

        if is_multi_tenant_workspace {
            let allows = options.dataset_visibility.is_publicly_available();
            let (name, value) = DatasetPropertyName::allows_public_read(allows);

            self.rebac_service
                .set_dataset_property(created_dataset_id, name, &value)
                .await
                .int_err()?;
        }

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    created_dataset_id.clone(),
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
                        created_dataset_id.clone(),
                        new_upstream_ids,
                    ),
                )
                .await?;
        }

        Ok(create_dataset_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
