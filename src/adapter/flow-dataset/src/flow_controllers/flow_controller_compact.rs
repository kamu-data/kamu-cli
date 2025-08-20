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
    FLOW_TYPE_DATASET_COMPACT,
    FlowConfigRuleCompact,
    FlowScopeDataset,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowController)]
#[dill::meta(fs::FlowControllerMeta {
    flow_type: FLOW_TYPE_DATASET_COMPACT,
})]
pub struct FlowControllerCompact {
    catalog: dill::Catalog,
    flow_sensor_dispatcher: Arc<dyn fs::FlowSensorDispatcher>,
}

#[async_trait::async_trait]
impl fs::FlowController for FlowControllerCompact {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_DATASET_COMPACT
    }

    #[tracing::instrument(name = "FlowControllerCompact::build_task_logical_plan", skip_all, fields(flow_id = %flow.flow_id))]
    async fn build_task_logical_plan(
        &self,
        flow: &fs::FlowState,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = FlowScopeDataset::new(&flow.flow_binding.scope).dataset_id();

        let mut max_slice_size: Option<u64> = None;
        let mut max_slice_records: Option<u64> = None;

        if let Some(config_snapshot) = flow.config_snapshot.as_ref()
            && config_snapshot.rule_type == FlowConfigRuleCompact::TYPE_ID
        {
            let compaction_rule = FlowConfigRuleCompact::from_flow_config(config_snapshot)?;
            max_slice_size = Some(compaction_rule.max_slice_size());
            max_slice_records = Some(compaction_rule.max_slice_records());
        }

        Ok(ats::LogicalPlanDatasetHardCompact {
            dataset_id,
            max_slice_size,
            max_slice_records,
        }
        .into_logical_plan())
    }

    #[tracing::instrument(name = "FlowControllerCompact::propagate_success", skip_all, fields(flow_id = %success_flow_state.flow_id))]
    async fn propagate_success(
        &self,
        success_flow_state: &fs::FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let compact_compaction_result =
            ats::TaskResultDatasetHardCompact::from_task_result(task_result)
                .int_err()?
                .compaction_result;

        match compact_compaction_result {
            CompactionResult::NothingToDo => {
                // No compaction was performed, no propagation needed
                tracing::debug!(flow_id=%success_flow_state.flow_id, "No compaction performed, skipping propagation");
                return Ok(());
            }
            CompactionResult::Success {
                old_head,
                new_head,
                old_num_blocks: _,
                new_num_blocks: _,
            } => {
                let dataset_id =
                    FlowScopeDataset::new(&success_flow_state.flow_binding.scope).dataset_id();

                tracing::debug!(flow_id=%success_flow_state.flow_id, %dataset_id, "Compaction successful, propagating changes");

                let activation_cause = fs::FlowActivationCause::ResourceUpdate(
                    fs::FlowActivationCauseResourceUpdate {
                        activation_time: finish_time,
                        changes: fs::ResourceChanges::Breaking,
                        resource_type: DATASET_RESOURCE_TYPE.to_string(),
                        details: serde_json::to_value(DatasetResourceUpdateDetails {
                            dataset_id: dataset_id.clone(),
                            source: DatasetUpdateSource::UpstreamFlow {
                                flow_id: success_flow_state.flow_id,
                                flow_type: success_flow_state.flow_binding.flow_type.clone(),
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

                // Trigger transform flows via sensors
                self.flow_sensor_dispatcher
                    .dispatch_input_flow_success(
                        &self.catalog,
                        &success_flow_state.flow_binding,
                        activation_cause,
                    )
                    .await
                    .int_err()?;

                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
