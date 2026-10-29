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
use internal_error::ErrorIntoInternal;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::*;
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
    DatasetRegistry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn SetWatermarkUseCase)]
pub struct SetWatermarkUseCaseImpl {
    set_watermark_planner: Arc<dyn SetWatermarkPlanner>,
    set_watermark_executor: Arc<dyn SetWatermarkExecutor>,
    dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    current_account_subject: Arc<CurrentAccountSubject>,
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
        self.dataset_operation_guard
            .check_operation(
                &self.current_account_subject,
                &DatasetOperationRequest::new(
                    dataset_handle.clone(),
                    DatasetOperation::SetWatermark,
                ),
            )
            .await
            .map_err(|e| {
                use DatasetPermissionError as E;

                match e {
                    E::NotFound(e) => SetWatermarkError::NotFound(e),
                    E::Access(e) => SetWatermarkError::Access(e),
                    e @ E::Internal(_) => SetWatermarkError::Internal(e.int_err()),
                }
            })?;

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
