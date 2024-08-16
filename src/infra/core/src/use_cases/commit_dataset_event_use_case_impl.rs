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
    CommitDatasetEventUseCase,
    CommitError,
    CommitOpts,
    CommitResult,
    DatasetLifecycleMessage,
    DatasetRepository,
    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
};
use messaging_outbox::{Outbox, OutboxExt};
use opendatafabric::{DatasetHandle, MetadataEvent};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CommitDatasetEventUseCase)]
pub struct CommitDatasetEventUseCaseImpl {
    dataset_repo: Arc<dyn DatasetRepository>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    outbox: Arc<dyn Outbox>,
}

impl CommitDatasetEventUseCaseImpl {
    pub fn new(
        dataset_repo: Arc<dyn DatasetRepository>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
        outbox: Arc<dyn Outbox>,
    ) -> Self {
        Self {
            dataset_repo,
            dataset_action_authorizer,
            outbox,
        }
    }
}

#[async_trait::async_trait]
impl CommitDatasetEventUseCase for CommitDatasetEventUseCaseImpl {
    async fn execute(
        &self,
        dataset_handle: &DatasetHandle,
        event: MetadataEvent,
        opts: CommitOpts<'_>,
    ) -> Result<CommitResult, CommitError> {
        self.dataset_action_authorizer
            .check_action_allowed(dataset_handle, DatasetAction::Write)
            .await?;

        let dataset = self.dataset_repo.get_dataset_by_handle(dataset_handle);

        let commit_result = dataset.commit_event(event, opts).await?;

        if !commit_result.new_upstream_ids.is_empty() {
            self.outbox
                .post_message(
                    MESSAGE_PRODUCER_KAMU_CORE_DATASET_SERVICE,
                    DatasetLifecycleMessage::dependencies_updated(
                        dataset_handle.id.clone(),
                        commit_result.new_upstream_ids.clone(),
                    ),
                )
                .await?;
        }

        Ok(commit_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
