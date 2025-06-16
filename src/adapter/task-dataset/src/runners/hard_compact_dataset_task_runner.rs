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

use crate::{TaskDefinitionDatasetHardCompact, TaskResultDatasetHardCompact};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component(pub)]
#[dill::interface(dyn TaskRunner)]
pub struct HardCompactDatasetTaskRunner {
    catalog: dill::Catalog,
    compaction_executor: Arc<dyn CompactionExecutor>,
}

impl HardCompactDatasetTaskRunner {
    #[tracing::instrument(level = "debug", skip_all, fields(?task_compact))]
    async fn run_hard_compact(
        &self,
        task_compact: TaskDefinitionDatasetHardCompact,
    ) -> Result<TaskOutcome, InternalError> {
        // Run compaction execution without transaction
        let compaction_result = self
            .compaction_executor
            .execute(
                task_compact.target.clone(),
                task_compact.compaction_plan,
                None,
            )
            .await;

        // Handle result
        match compaction_result {
            // Compaction finished without errors
            Ok(compaction_result) => {
                // Do we have a new HEAD suggestion?
                if let CompactionResult::Success {
                    old_head, new_head, ..
                } = &compaction_result
                {
                    // Update the reference transactionally
                    self.update_dataset_head(
                        task_compact.target.get_handle(),
                        Some(old_head),
                        new_head,
                    )
                    .await?;
                }

                Ok(TaskOutcome::Success(
                    TaskResultDatasetHardCompact { compaction_result }.into_task_result(),
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

    #[tracing::instrument(level = "debug", skip_all, fields(%dataset_handle, %new_head))]
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
impl TaskRunner for HardCompactDatasetTaskRunner {
    fn supported_task_type(&self) -> &str {
        TaskDefinitionDatasetHardCompact::TASK_TYPE
    }

    async fn run_task(
        &self,
        task_definition: kamu_task_system::TaskDefinition,
    ) -> Result<kamu_task_system::TaskOutcome, kamu_task_system::InternalError> {
        let task_compact = task_definition
            .downcast::<TaskDefinitionDatasetHardCompact>()
            .expect("Mismatched task type for HardCompactDatasetTaskRunner");

        self.run_hard_compact(*task_compact).await
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
