// Copyright Kamu Data, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::sync::Arc;

use database_common_macros::transactional_method1;
use internal_error::InternalError;
use kamu_core::{CompactionExecutor, CompactionResult, DatasetRegistry};
use kamu_task_system::*;

use crate::{TaskDefinitionDatasetResetToMetadata, TaskResultDatasetResetToMetadata};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskRunner)]
#[dill::meta(TaskRunnerMeta {
    task_type: TaskDefinitionDatasetResetToMetadata::TASK_TYPE,
})]
pub struct ResetDatasetToMetadataTaskRunner {
    catalog: dill::Catalog,
    compaction_executor: Arc<dyn CompactionExecutor>,
}

#[common_macros::method_names_consts]
impl ResetDatasetToMetadataTaskRunner {
    #[tracing::instrument(name = ResetDatasetToMetadataTaskRunner_run_reset_to_metadata, level = "debug", skip_all)]
    async fn run_reset_to_metadata(
        &self,
        task_reset: TaskDefinitionDatasetResetToMetadata,
    ) -> Result<TaskOutcome, InternalError> {
        tracing::debug!(?task_reset, "Running reset to metadata task");

        // Run compaction execution without transaction
        let compaction_metadata_only_result = self
            .compaction_executor
            .execute(
                task_reset.target.clone(),
                task_reset.compaction_metadata_only_plan,
                None,
            )
            .await;

        // Handle result
        match compaction_metadata_only_result {
            // Compaction finished without errors
            Ok(compaction_result) => {
                // Do we have a new HEAD suggestion?
                if let CompactionResult::Success {
                    old_head, new_head, ..
                } = &compaction_result
                {
                    // Update the reference transactionally
                    self.update_dataset_head(
                        task_reset.target.get_handle(),
                        Some(old_head),
                        new_head,
                    )
                    .await?;
                }

                Ok(TaskOutcome::Success(
                    TaskResultDatasetResetToMetadata {
                        compaction_metadata_only_result: compaction_result,
                    }
                    .into_task_result(),
                ))
            }

            // Compaction failed
            Err(err) => {
                tracing::error!(
                    error = ?err,
                    error_msg = %err,
                    "Hard compaction failed",
                );

                Ok(TaskOutcome::Failed(TaskError::empty()))
            }
        }
    }

    #[tracing::instrument(name =ResetDatasetToMetadataTaskRunner_update_dataset_head, level = "debug", skip_all, fields(%dataset_handle, %new_head))]
    #[transactional_method1(dataset_registry: Arc<dyn DatasetRegistry>)]
    async fn update_dataset_head(
        &self,
        dataset_handle: &odf::DatasetHandle,
        old_head: Option<&odf::Multihash>,
        new_head: &odf::Multihash,
    ) -> Result<(), InternalError> {
        let target = dataset_registry.get_dataset_by_handle(dataset_handle).await;

        target
            .as_metadata_chain()
            .set_ref(
                &odf::BlockRef::Head,
                new_head,
                odf::dataset::SetRefOpts {
                    validate_block_present: true,
                    check_ref_is: Some(old_head),
                },
            )
            .await
            .int_err()
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[async_trait::async_trait]
impl TaskRunner for ResetDatasetToMetadataTaskRunner {
    async fn run_task(
        &self,
        task_definition: kamu_task_system::TaskDefinition,
    ) -> Result<kamu_task_system::TaskOutcome, kamu_task_system::InternalError> {
        let task_reset = task_definition
            .downcast::<TaskDefinitionDatasetResetToMetadata>()
            .expect("Mismatched task type for ResetDatasetToMetadataTaskRunner");

        self.run_reset_to_metadata(*task_reset).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
