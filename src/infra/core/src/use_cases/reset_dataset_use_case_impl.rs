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
use internal_error::ErrorIntoInternal;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{ResetDatasetUseCase, ResetError, ResetExecutor, ResetPlanner, ResetResult};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
    DatasetRegistry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component]
#[interface(dyn ResetDatasetUseCase)]
pub struct ResetDatasetUseCaseImpl {
    reset_planner: Arc<dyn ResetPlanner>,
    reset_executor: Arc<dyn ResetExecutor>,
    dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl ResetDatasetUseCase for ResetDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = ResetDatasetUseCaseImpl_execute,
        skip_all,
        fields(dataset_handle, ?maybe_new_head, ?maybe_old_head)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        maybe_new_head: Option<&odf::Multihash>,
        maybe_old_head: Option<&odf::Multihash>,
    ) -> Result<ResetResult, ResetError> {
        self.dataset_operation_guard
            .check_operation(
                &self.current_account_subject,
                &DatasetOperationRequest::new(dataset_handle.clone(), DatasetOperation::Reset),
            )
            .await
            .map_err(|e| {
                use DatasetPermissionError as E;

                match e {
                    E::NotFound(e) => ResetError::NotFound(e),
                    E::Access(e) => ResetError::Access(e),
                    e @ E::Internal(_) => ResetError::Internal(e.int_err()),
                }
            })?;

        let target = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await;

        // Make a plan
        let reset_plan = self
            .reset_planner
            .plan_reset(target.clone(), maybe_new_head, maybe_old_head)
            .await?;

        // Execute the plan
        let reset_result = self.reset_executor.execute(target, reset_plan).await?;

        Ok(reset_result)
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
