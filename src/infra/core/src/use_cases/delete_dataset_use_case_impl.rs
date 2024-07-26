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
use kamu_core::{
    DatasetDeletedMessage,
    DatasetRepository,
    DeleteDatasetError,
    DeleteDatasetUseCase,
    GetDatasetError,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{post_outbox_message, MessageRelevance, Outbox};
use opendatafabric::DatasetRef;

use crate::DatasetRepositoryWriter;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn DeleteDatasetUseCase)]
pub struct DeleteDatasetUseCaseImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    outbox: Arc<dyn Outbox>,
}

impl DeleteDatasetUseCaseImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_repo_writer: Arc<dyn DatasetRepositoryWriter>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_repo_writer,
            dataset_action_authorizer,
            outbox,
        }
    }
}

#[async_trait::async_trait]
impl DeleteDatasetUseCase for DeleteDatasetUseCaseImpl {
    async fn execute(&self, dataset_ref: &DatasetRef) -> Result<(), DeleteDatasetError> {
        let dataset_handle = match self.dataset_repo.resolve_dataset_ref(dataset_ref).await {
            Ok(h) => Ok(h),
            Err(GetDatasetError::NotFound(e)) => Err(DeleteDatasetError::NotFound(e)),
            Err(GetDatasetError::Internal(e)) => Err(DeleteDatasetError::Internal(e)),
        }?;

        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle, DatasetAction::Write)
            .await?;

        self.dataset_repo_writer.delete_dataset(dataset_ref).await?;

        let message = DatasetDeletedMessage {
            dataset_id: dataset_handle.id,
        };

        post_outbox_message(
            self.outbox.as_ref(),
            MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
            message,
            MessageRelevance::Essential,
        )
        .await?;

        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
