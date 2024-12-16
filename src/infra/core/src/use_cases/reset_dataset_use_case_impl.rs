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
    DatasetRegistry,
    ResetDatasetUseCase,
    ResetError,
    ResetExecutionService,
    ResetPlanner,
    ResetResult,
};
use opendatafabric::{DatasetHandle, Multihash};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn ResetDatasetUseCase)]
pub struct ResetDatasetUseCaseImpl {
    reset_planner: Arc<dyn ResetPlanner>,
    reset_execution_service: Arc<dyn ResetExecutionService>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl ResetDatasetUseCaseImpl {
    pub fn new(
        reset_planner: Arc<dyn ResetPlanner>,
        reset_execution_service: Arc<dyn ResetExecutionService>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            reset_planner,
            reset_execution_service,
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
    ) -> Result<ResetResult, ResetError> {
        // Permission check
        self.dataset_action_authorizer
            .check_action_allowed(dataset_handle, DatasetAction::Write)
            .await?;

        // Resolve dataset
        let target = self.dataset_registry.get_dataset_by_handle(dataset_handle);

        // Make a plan
        let reset_plan = self
            .reset_planner
            .build_reset_plan(target.clone(), maybe_new_head, maybe_old_head)
            .await?;

        // Execute the plan
        let reset_result = self
            .reset_execution_service
            .execute_reset(target, reset_plan)
            .await?;

        Ok(reset_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
