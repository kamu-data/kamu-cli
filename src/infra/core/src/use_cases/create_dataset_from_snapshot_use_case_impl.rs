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
use kamu_core::{
    CreateDatasetFromSnapshotError,
    CreateDatasetFromSnapshotResult,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    CreateDatasetUseCaseOptions,
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
    outbox: Arc<dyn Outbox>,
}

impl CreateDatasetFromSnapshotUseCaseImpl {
    pub fn new(
        current_account_subject: Arc<CurrentAccountSubject>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            current_account_subject,
            dataset_repo_writer,
            outbox,
        }
    }
}

#[async_trait::async_trait]
impl CreateDatasetFromSnapshotUseCase for CreateDatasetFromSnapshotUseCaseImpl {
    #[tracing::instrument(level = "info", skip_all, fields(?snapshot, ?options))]
    async fn execute(
        &self,
        snapshot: DatasetSnapshot,
        options: CreateDatasetUseCaseOptions,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        let logged_account_id = match self.current_account_subject.as_ref() {
            CurrentAccountSubject::Anonymous(_) => {
                panic!("Anonymous account cannot create dataset");
            }
            CurrentAccountSubject::Logged(l) => l.account_id.clone(),
        };
        let dataset_name = snapshot.name.dataset_name.clone();
        let CreateDatasetFromSnapshotResult {
            create_dataset_result,
            new_upstream_ids,
        } = self
            .dataset_repo_writer
            .create_dataset_from_snapshot(snapshot)
            .await?;

        self.outbox
            .post_message(
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                DatasetLifecycleMessage::created(
                    create_dataset_result.dataset_handle.id.clone(),
                    logged_account_id,
                    options.dataset_visibility,
                    dataset_name,
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
