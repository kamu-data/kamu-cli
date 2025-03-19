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
    CompactDatasetUseCase,
    CompactionError,
    CompactionExecutor,
    CompactionListener,
    CompactionMultiListener,
    CompactionOptions,
    CompactionPlanner,
    CompactionResponse,
    CompactionResult,
    DatasetRegistry,
    NullCompactionMultiListener,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[component(pub)]
#[interface(dyn CompactDatasetUseCase)]
pub struct CompactDatasetUseCaseImpl {
    compaction_planner: Arc<dyn CompactionPlanner>,
    compaction_executor: Arc<dyn CompactionExecutor>,
    dataset_registry: Arc<dyn DatasetRegistry>,
    dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
}

impl CompactDatasetUseCaseImpl {
    pub fn new(
        compaction_planner: Arc<dyn CompactionPlanner>,
        compaction_executor: Arc<dyn CompactionExecutor>,
        dataset_registry: Arc<dyn DatasetRegistry>,
        dataset_action_authorizer: Arc<dyn DatasetActionAuthorizer>,
    ) -> Self {
        Self {
            compaction_planner,
            compaction_executor,
            dataset_registry,
            dataset_action_authorizer,
        }
    }
}

#[async_trait::async_trait]
impl CompactDatasetUseCase for CompactDatasetUseCaseImpl {
    #[tracing::instrument(
        level = "info",
        name = "CompactDatasetUseCase::execute",
        skip_all,
        fields(dataset_handle, ?options)
    )]
    async fn execute(
        &self,
        dataset_handle: &odf::DatasetHandle,
        options: CompactionOptions,
        maybe_listener: Option<Arc<dyn CompactionListener>>,
    ) -> Result<CompactionResult, CompactionError> {
        // Permission check
        self.dataset_action_authorizer
            .check_action_allowed(&dataset_handle.id, DatasetAction::Write)
            .await?;

        // Resolve dataset
        let target = self
            .dataset_registry
            .get_dataset_by_handle(dataset_handle)
            .await;

        // Plan compacting
        let compaction_plan = self
            .compaction_planner
            .plan_compaction(target.clone(), options, maybe_listener.clone())
            .await?;

        // Execute compacting
        let compaction_result = self
            .compaction_executor
            .execute(target.clone(), compaction_plan, maybe_listener)
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
                target
                    .as_metadata_chain()
                    .set_ref(
                        &odf::BlockRef::Head,
                        new_head,
                        odf::dataset::SetRefOpts {
                            validate_block_present: true,
                            check_ref_is: Some(Some(old_head)),
                        },
                    )
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
