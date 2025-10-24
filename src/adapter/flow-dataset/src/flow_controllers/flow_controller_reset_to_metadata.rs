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
use internal_error::{InternalError, ResultIntoInternal};
use kamu_core::CompactionResult;
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    DATASET_RESOURCE_TYPE,
    DatasetResourceUpdateDetails,
    DatasetUpdateSource,
    FLOW_TYPE_DATASET_RESET_TO_METADATA,
    FlowScopeDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowController)]
#[dill::meta(fs::FlowControllerMeta {
    flow_type: FLOW_TYPE_DATASET_RESET_TO_METADATA,
})]
pub struct FlowControllerResetToMetadata {
    catalog: dill::Catalog,
    flow_sensor_dispatcher: Arc<dyn fs::FlowSensorDispatcher>,
}

#[async_trait::async_trait]
impl fs::FlowController for FlowControllerResetToMetadata {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_DATASET_RESET_TO_METADATA
    }

    #[tracing::instrument(name = "FlowControllerResetToMetadata::build_task_logical_plan", skip_all, fields(flow_id = %flow.flow_id))]
    async fn build_task_logical_plan(
        &self,
        flow: &fs::FlowState,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = FlowScopeDataset::new(&flow.flow_binding.scope).dataset_id();
        Ok(ats::LogicalPlanDatasetResetToMetadata { dataset_id }.into_logical_plan())
    }

    #[tracing::instrument(
        name = "FlowControllerResetToMetadata::propagate_success", skip_all, fields(flow_id = %success_flow_state.flow_id)
    )]
    async fn propagate_success(
        &self,
        success_flow_state: &fs::FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<bool, InternalError> {
        let reset_to_metadata_only_result =
            ats::TaskResultDatasetResetToMetadata::from_task_result(task_result)
                .int_err()?
                .compaction_metadata_only_result;

        match reset_to_metadata_only_result {
            CompactionResult::NothingToDo => {
                // No changes performed, no propagation needed
                tracing::debug!(flow_id = %success_flow_state.flow_id, "No reset to metadata performed, skipping propagation");
                return Ok(true);
            }
            CompactionResult::Success {
                old_head,
                new_head,
                old_num_blocks: _,
                new_num_blocks: _,
            } => {
                // Reset to metadata is a breaking operation.
                // Datasets with enabled updates would compact to metadata only.
                // Other datasets will stay unaffected, but would break on next update attempt
                let dataset_id =
                    FlowScopeDataset::new(&success_flow_state.flow_binding.scope).dataset_id();

                tracing::debug!(flow_id = %success_flow_state.flow_id, %dataset_id, "Reset to metadata successful, propagating changes");

                let activation_cause = fs::FlowActivationCause::ResourceUpdate(
                    fs::FlowActivationCauseResourceUpdate {
                        activation_time: finish_time,
                        changes: fs::ResourceChanges::Breaking,
                        resource_type: DATASET_RESOURCE_TYPE.to_string(),
                        details: serde_json::to_value(DatasetResourceUpdateDetails {
                            dataset_id: dataset_id.clone(),
                            source: DatasetUpdateSource::UpstreamFlow {
                                flow_type: success_flow_state.flow_binding.flow_type.clone(),
                                flow_id: success_flow_state.flow_id,
                                maybe_flow_config_snapshot: success_flow_state
                                    .config_snapshot
                                    .clone(),
                            },
                            new_head,
                            old_head_maybe: Some(old_head),
                        })
                        .int_err()?,
                    },
                );

                self.flow_sensor_dispatcher
                    .dispatch_input_flow_success(
                        &self.catalog,
                        &success_flow_state.flow_binding,
                        activation_cause,
                    )
                    .await
                    .int_err()?;

                Ok(true)
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
