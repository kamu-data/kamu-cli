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
use kamu_core::{DatasetRegistry, ResetDatasetUseCase, ResetError, ResetService};
use opendatafabric::{DatasetHandle, Multihash};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ResetDatasetUseCase)]
pub struct ResetDatasetUseCaseImpl {
    reset_service: Arc<dyn ResetService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl ResetDatasetUseCaseImpl {
    pub fn new(
        reset_service: Arc<dyn ResetService>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            reset_service,
            dataset_registry,
            dataset_action_authorizer,
        }
    }
}

#[async_trait::async_trait]
impl ResetDatasetUseCase for ResetDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "ResetDatasetUseCase::execute",
        skip_all,
        fields(dataset_handle, ?maybe_new_head, ?maybe_old_head)
    )]
    async fn execute(
        &self,
        dataset_handle: &DatasetHandle,
        maybe_new_head: Option<&Multihash>,
        maybe_old_head: Option<&Multihash>,
    ) -> Result<Multihash, ResetError> {
        // Permission check
        self.dataset_action_authorizer
            .check_action_allowed(dataset_handle, DatasetAction::Write)
            .await?;

        // Resolve dataset
        let dataset = self.dataset_registry.get_dataset_by_handle(dataset_handle);

        // Actual action
        self.reset_service
            .reset_dataset(dataset, maybe_new_head, maybe_old_head)
            .await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
