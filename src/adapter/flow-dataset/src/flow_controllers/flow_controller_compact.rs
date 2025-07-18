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
use kamu_datasets::{DatasetEntryService, DependencyGraphService};
use kamu_flow_system::FlowRunService;
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    FLOW_TYPE_DATASET_COMPACT,
    FlowConfigRuleCompact,
    trigger_metadata_only_hard_compaction_flow_for_own_downstream_datasets,
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
    // TODO: try to avoid this
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    flow_run_service: Arc<dyn FlowRunService>,
}

#[async_trait::async_trait]
impl fs::FlowController for FlowControllerCompact {
    fn flow_type(&self) -> &'static str {
        FLOW_TYPE_DATASET_COMPACT
    }

    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
        _maybe_task_run_arguments: Option<&ts::TaskRunArguments>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = flow_binding.get_dataset_id_or_die()?;

        let mut max_slice_size: Option<u64> = None;
        let mut max_slice_records: Option<u64> = None;
        let mut keep_metadata_only = false;

        if let Some(config_snapshot) = maybe_config_snapshot
            && config_snapshot.rule_type == FlowConfigRuleCompact::TYPE_ID
        {
            let compaction_rule = FlowConfigRuleCompact::from_flow_config(config_snapshot)?;
            max_slice_size = compaction_rule.max_slice_size();
            max_slice_records = compaction_rule.max_slice_records();
            keep_metadata_only =
                matches!(compaction_rule, FlowConfigRuleCompact::MetadataOnly { .. });
        }

        Ok(ats::LogicalPlanDatasetHardCompact {
            dataset_id,
            max_slice_size,
            max_slice_records,
            keep_metadata_only,
        }
        .into_logical_plan())
    }

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
                return Ok(());
            }
            CompactionResult::Success {
                old_head,
                new_head,
                old_num_blocks: _,
                new_num_blocks: _,
            } => {
                let dataset_id = success_flow_state.flow_binding.get_dataset_id_or_die()?;

                let activation_cause =
                    fs::FlowActivationCause::DatasetUpdate(fs::FlowActivationCauseDatasetUpdate {
                        activation_time: finish_time,
                        dataset_id,
                        source: fs::DatasetUpdateSource::UpstreamFlow {
                            flow_id: success_flow_state.flow_id,
                            flow_type: success_flow_state.flow_binding.flow_type.clone(),
                            maybe_flow_config_snapshot: success_flow_state.config_snapshot.clone(),
                        },
                        new_head,
                        old_head_maybe: Some(old_head),
                        changes: fs::DatasetChanges::Breaking,
                    });

                if let Some(config_snapshot) = success_flow_state.config_snapshot.as_ref()
                    && config_snapshot.rule_type == FlowConfigRuleCompact::TYPE_ID
                {
                    let dataset_id = success_flow_state.flow_binding.get_dataset_id_or_die()?;

                    let compaction_rule = FlowConfigRuleCompact::from_flow_config(config_snapshot)?;
                    if compaction_rule.recursive() {
                        trigger_metadata_only_hard_compaction_flow_for_own_downstream_datasets(
                            self.dataset_entry_service.as_ref(),
                            self.dependency_graph_service.as_ref(),
                            self.flow_run_service.as_ref(),
                            &dataset_id,
                            activation_cause,
                        )
                        .await?;
                    } else {
                        // Nothing to do here, non-recursive compaction
                    }
                } else {
                    // Trigger transform flows normally via sensors
                    self.flow_sensor_dispatcher
                        .dispatch_input_flow_success(
                            &self.catalog,
                            &success_flow_state.flow_binding,
                            activation_cause,
                        )
                        .await
                        .int_err()?;
                }

                Ok(())
            }
        }
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
