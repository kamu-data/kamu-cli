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
use kamu_core::messages::DatasetCreatedMessage;
use kamu_core::{
    CreateDatasetFromSnapshotError,
    CreateDatasetFromSnapshotResult,
    CreateDatasetFromSnapshotUseCase,
    CreateDatasetResult,
    DatasetDependenciesUpdatedMessage,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{post_outbox_message, MessageRelevance, Outbox};
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
    async fn execute(
        &self,
        snapshot: DatasetSnapshot,
    ) -> Result<CreateDatasetResult, CreateDatasetFromSnapshotError> {
        let CreateDatasetFromSnapshotResult {
            create_dataset_result,
            new_upstream_ids,
        } = self
            .dataset_repo_writer
            .create_dataset_from_snapshot(snapshot)
            .await?;

        let message_created = DatasetCreatedMessage {
            dataset_id: create_dataset_result.dataset_handle.id.clone(),
            owner_account_id: match self.current_account_subject.as_ref() {
                CurrentAccountSubject::Anonymous(_) => {
                    panic!("Anonymous account cannot create dataset");
                }
                CurrentAccountSubject::Logged(l) => l.account_id.clone(),
            },
        };

        post_outbox_message(
            self.outbox.as_ref(),
            MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
            message_created,
            MessageRelevance::Essential,
        )
        .await?;

        if !new_upstream_ids.is_empty() {
            let message_dependencies_updated = DatasetDependenciesUpdatedMessage {
                dataset_id: create_dataset_result.dataset_handle.id.clone(),
                new_upstream_ids,
            };

            post_outbox_message(
                self.outbox.as_ref(),
                MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                message_dependencies_updated,
                MessageRelevance::Essential,
            )
            .await?;
        }

        Ok(create_dataset_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
