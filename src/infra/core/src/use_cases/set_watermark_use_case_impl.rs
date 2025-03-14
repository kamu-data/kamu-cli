// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use dill::{component, interface};
use kamu_core::auth::{DatasetAction, DatasetActionAuthorizer};
use kamu_core::*;

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn SetWatermarkUseCase)]
pub struct SetWatermarkUseCaseImpl {
    set_watermark_planner: Arc<dyn SetWatermarkPlanner>,
    set_watermark_executor: Arc<dyn SetWatermarkExecutor>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl SetWatermarkUseCaseImpl {
    pub fn new(
        set_watermark_planner: Arc<dyn SetWatermarkPlanner>,
        set_watermark_executor: Arc<dyn SetWatermarkExecutor>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            set_watermark_planner,
            set_watermark_executor,
            dataset_registry,
            dataset_action_authorizer,
        }
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl SetWatermarkUseCase for SetWatermarkUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = SetWatermarkUseCaseImpl_execute,
        skip_all,
        fields(dataset_handle, new_watermark)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        new_watermark: DateTime<Utc>,
    ) -> Result<SetWatermarkResult, SetWatermarkError> {
        // Permission check
        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle.id, DatasetAction::Write)
            .await?;

        // Resolve dataset
        let target = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await;

        // Make a plan
        let plan = self
            .set_watermark_planner
            .plan_set_watermark(target.clone(), new_watermark)
            .await?;

        // Execute the plan
        let result = self.set_watermark_executor.execute(target, plan).await?;

        Ok(result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
