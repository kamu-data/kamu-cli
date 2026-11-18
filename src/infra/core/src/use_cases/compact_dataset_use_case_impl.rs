// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common::transaction;
use dill::{component, interface};
use internal_error::ErrorIntoInternal;
use kamu_accounts::Principal;
use kamu_core::{
    CompactDatasetUseCase,
    CompactionError,
    CompactionExecutor,
    CompactionListener,
    CompactionMultiListener,
    CompactionOptions,
    CompactionPlanner,
    CompactionResponse,
    CompactionResult,
    NullCompactionListener,
    NullCompactionMultiListener,
};
use kamu_datasets::{
    DatasetOperation,
    DatasetOperationGuard,
    DatasetOperationRequest,
    DatasetPermissionError,
    DatasetReferenceUpdater,
    DatasetRegistry,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

/// Plans and commits in short transactions of their own and runs the merge
/// with no transaction open, so a long compaction blocks no other writer.
#[component]
#[interface(dyn CompactDatasetUseCase)]
pub struct CompactDatasetUseCaseImpl {
    catalog: dill::Catalog,
    compaction_executor: Arc<dyn CompactionExecutor>,
    principal: Arc<Principal>,
}

impl CompactDatasetUseCaseImpl {
    async fn compact_and_commit(
        &self,
        dataset_handle: &odf::DatasetHandle,
        options: CompactionOptions,
        listener: Arc<dyn CompactionListener>,
    ) -> Result<CompactionResult, CompactionError> {
        // 1: Plan
        let (target, compaction_plan) =
            transaction!(
                |dataset_operation_guard: Arc<dyn DatasetOperationGuard>,
                 dataset_registry: Arc<dyn DatasetRegistry>,
                 compaction_planner: Arc<dyn CompactionPlanner>| {
                    self.check_compaction_allowed(
                        dataset_operation_guard.as_ref(),
                        dataset_handle,
                        &options,
                    )
                    .await?;

                    let target = dataset_registry.get_dataset_by_handle(dataset_handle).await;
                    let compaction_plan = compaction_planner
                        .plan_compaction(target.clone(), options, Some(listener.clone()))
                        .await?;
                    target.detach_from_transaction();

                    (target, compaction_plan)
                }
            );

        // 2: Merge, with no transaction open
        let compaction_result = self
            .compaction_executor
            .execute(target, compaction_plan, Some(listener))
            .await?;

        // 3: Commit, if something got compacted
        match &compaction_result {
            CompactionResult::NothingToDo => {
                tracing::debug!(%dataset_handle, "Skipping setting reference. Dataset was not compacted");
            }
            CompactionResult::Success {
                old_head, new_head, ..
            } => {
                tracing::debug!(%dataset_handle, %new_head, "Setting new compacted head");
                transaction!(
                    |dataset_registry: Arc<dyn DatasetRegistry>,
                     dataset_reference_updater: Arc<dyn DatasetReferenceUpdater>| {
                        let target = dataset_registry.get_dataset_by_handle(dataset_handle).await;
                        dataset_reference_updater
                            .update_reference(
                                &target,
                                &odf::BlockRef::Head,
                                Some(old_head),
                                new_head,
                            )
                            .await?;
                    }
                );
            }
        }

        Ok(compaction_result)
    }

    async fn check_compaction_allowed(
        &self,
        dataset_operation_guard: &dyn DatasetOperationGuard,
        dataset_handle: &odf::DatasetHandle,
        options: &CompactionOptions,
    ) -> Result<(), CompactionError> {
        let operation = if options.keep_metadata_only {
            DatasetOperation::ResetToMetadata
        } else {
            DatasetOperation::Compaction
        };
        dataset_operation_guard
            .check_operation(
                &self.principal,
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
            })
    }
}

#[common_macros::method_names_consts]
#[async_trait::async_trait]
impl CompactDatasetUseCase for CompactDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = CompactDatasetUseCaseImpl_execute,
        skip_all,
        fields(target = %dataset_handle, ?options)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionError> {
        let listener = maybe_listener.unwrap_or_else(|| Arc::new(NullCompactionListener {}));
        listener.begin();

        let result = self
            .compact_and_commit(dataset_handle, options, listener.clone())
            .await;
        match &result {
            Ok(compaction_result) => listener.success(compaction_result),
            Err(e) => listener.error(e),
        }
        result
    }

    #[tracing::instrument(
        level = "info",
        name = CompactDatasetUseCaseImpl_execute_multi,
        skip_all,
        fields(num_datasets = dataset_handles.len(), ?options)
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
