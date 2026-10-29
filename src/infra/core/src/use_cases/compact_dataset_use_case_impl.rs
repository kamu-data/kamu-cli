// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::{transactional_method2, transactional_method3};
use dill::{component, interface};
use internal_error::ErrorIntoInternal;
use kamu_accounts::CurrentAccountSubject;
use kamu_core::{
    CompactDatasetUseCase,
    CompactionError,
    CompactionExecutor,
    CompactionListener,
    CompactionMultiListener,
    CompactionOptions,
    CompactionPlan,
    CompactionPlanner,
    CompactionResponse,
    CompactionResult,
    NullCompactionMultiListener,
};
use kamu_datasets::{
    DatasetHeadCommitter,
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
    DatasetRegistry,
    ResolvedDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plans and commits in short transactions of their own and runs the merge
/// with no transaction open, so a long compaction blocks no other writer.
#[component]
#[interface(dyn CompactDatasetUseCase)]
pub struct CompactDatasetUseCaseImpl {
    catalog: dill::Catalog,
    compaction_executor: Arc<dyn CompactionExecutor>,
    current_account_subject: Arc<CurrentAccountSubject>,
}

impl CompactDatasetUseCaseImpl {
    #[transactional_method3(dataset_operation_guard: Arc<dyn DatasetOperationGuard>, dataset_registry: Arc<dyn DatasetRegistry>, compaction_planner: Arc<dyn CompactionPlanner>)]
    async fn plan_transactionally(
        &self,
        dataset_handle: &odf::DatasetHandle,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<(ResolvedDataset, CompactionPlan), CompactionError> {
        let operation = if options.keep_metadata_only {
            DatasetOperation::ResetToMetadata
        } else {
            DatasetOperation::Compaction
        };
        dataset_operation_guard
            .check_operation(
                &self.current_account_subject,
                &DatasetOperationRequest::new(dataset_handle.clone(), operation),
            )
            .await
            .map_err(|e| {
                use DatasetPermissionError as E;

                match e {
                    E::NotFound(e) => CompactionError::NotFound(e),
                    E::Access(e) => CompactionError::Access(e),
                    e @ E::Internal(_) => CompactionError::Internal(e.int_err()),
                }
            })?;

        let target = dataset_registry.get_dataset_by_handle(dataset_handle).await;

        let compaction_plan = compaction_planner
            .plan_compaction(target.clone(), options, maybe_listener)
            .await?;

        target.detach_from_transaction();

        Ok((target, compaction_plan))
    }

    #[transactional_method2(dataset_registry: Arc<dyn DatasetRegistry>, dataset_head_committer: Arc<dyn DatasetHeadCommitter>)]
    async fn commit_head_transactionally(
        &self,
        dataset_handle: &odf::DatasetHandle,
        old_head: &odf::Multihash,
        new_head: &odf::Multihash,
    ) -> Result<(), odf::dataset::SetChainRefError> {
        let target = dataset_registry.get_dataset_by_handle(dataset_handle).await;

        dataset_head_committer
            .commit_head(&target, Some(old_head), new_head)
            .await
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CompactDatasetUseCase for CompactDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = CompactDatasetUseCaseImpl_execute,
        skip_all,
        fields(dataset_handle, ?options)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionError> {
        let (target, compaction_plan) = self
            .plan_transactionally(dataset_handle, options, maybe_listener.clone())
            .await?;

        let compaction_result = self
            .compaction_executor
            .execute(target, compaction_plan, maybe_listener)
            .await?;

        // Set proposed reference, if something got compacted
        match &compaction_result {
            CompactionResult::NothingToDo => {
                tracing::debug!(%dataset_handle, "Skipping setting reference. Dataset was not compacted");
            }
            CompactionResult::Success {
                old_head, new_head, ..
            } => {
                tracing::debug!(%dataset_handle, %new_head, "Setting new compacted head");
                self.commit_head_transactionally(dataset_handle, old_head, new_head)
                    .await?;
            }
        }

        Ok(compaction_result)
    }

    #[tracing::instrument(
        level = "info",
        name = "CompactDatasetUseCase::execute_multi",
        skip_all,
        fields(?dataset_handles, ?options)
    )]
    async fn execute_multi(
        &self,
        dataset_handles: Vec<odf::DatasetHandle>,
        options: CompactionOptions,
        multi_listener: Option<Arc<dyn CompactionMultiListener>>,
    ) -> Vec<CompactionResponse> {
        let listener = multi_listener.unwrap_or(Arc::new(NullCompactionMultiListener {}));

        let mut result = vec![];
        for dataset_handle in &dataset_handles {
            result.push(CompactionResponse {
                dataset_ref: dataset_handle.as_local_ref(),
                result: self
                    .execute(
                        dataset_handle,
                        options.clone(),
                        listener.begin_compact(dataset_handle),
                    )
                    .await,
            });
        }
        result
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
