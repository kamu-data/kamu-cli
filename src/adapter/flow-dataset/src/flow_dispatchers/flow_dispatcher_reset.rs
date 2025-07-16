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
use kamu_datasets::{DatasetEntryService, DependencyGraphService};
use {kamu_adapter_task_dataset as ats, kamu_flow_system as fs, kamu_task_system as ts};

use crate::{
    FLOW_TYPE_DATASET_RESET,
    FlowConfigRuleReset,
    trigger_metadata_only_hard_compaction_flow_for_own_downstream_datasets,
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

#[dill::component]
#[dill::interface(dyn fs::FlowDispatcher)]
#[dill::meta(fs::FlowDispatcherMeta {
    flow_type: FLOW_TYPE_DATASET_RESET,
})]
pub struct FlowDispatcherReset {
    dataset_entry_service: Arc<dyn DatasetEntryService>,
    dependency_graph_service: Arc<dyn DependencyGraphService>,
    flow_run_service: Arc<dyn fs::FlowRunService>,
}

#[async_trait::async_trait]
impl fs::FlowDispatcher for FlowDispatcherReset {
    async fn build_task_logical_plan(
        &self,
        flow_binding: &fs::FlowBinding,
        maybe_config_snapshot: Option<&fs::FlowConfigurationRule>,
        _maybe_task_run_arguments: Option<&ts::TaskRunArguments>,
    ) -> Result<ts::LogicalPlan, InternalError> {
        let dataset_id = flow_binding.get_dataset_id_or_die()?;

        if let Some(config_snapshot) = maybe_config_snapshot
            && config_snapshot.rule_type == FlowConfigRuleReset::TYPE_ID
        {
            let reset_rule = FlowConfigRuleReset::from_flow_config(config_snapshot)?;
            Ok(ats::LogicalPlanDatasetReset {
                dataset_id,
                new_head_hash: reset_rule.new_head_hash,
                old_head_hash: reset_rule.old_head_hash,
                recursive: reset_rule.recursive,
            }
            .into_logical_plan())
        } else {
            InternalError::bail("Reset flow cannot be called without configuration")
        }
    }

    async fn propagate_success(
        &self,
        success_flow_state: &fs::FlowState,
        task_result: &ts::TaskResult,
        finish_time: DateTime<Utc>,
    ) -> Result<(), InternalError> {
        let reset_result = ats::TaskResultDatasetReset::from_task_result(task_result)
            .int_err()?
            .reset_result;

        if reset_result
            .old_head
            .as_ref()
            .is_some_and(|old_head| *old_head == reset_result.new_head)
        {
            // No reset was performed, no propagation needed
            return Ok(());
        }

        let activation_cause =
            fs::FlowActivationCause::DatasetUpdate(fs::FlowActivationCauseDatasetUpdate {
                activation_time: finish_time,
                dataset_id: success_flow_state.flow_binding.get_dataset_id_or_die()?,
                source: fs::DatasetUpdateSource::UpstreamFlow {
                    flow_type: success_flow_state.flow_binding.flow_type.clone(),
                    flow_id: success_flow_state.flow_id,
                },
                new_head: reset_result.new_head,
                old_head_maybe: reset_result.old_head,
                changes: fs::DatasetChanges::Breaking,
            });

        if let Some(config_snapshot) = success_flow_state.config_snapshot.as_ref()
            && config_snapshot.rule_type == FlowConfigRuleReset::TYPE_ID
        {
            let dataset_id = success_flow_state.flow_binding.get_dataset_id_or_die()?;

            let reset_rule = FlowConfigRuleReset::from_flow_config(config_snapshot)?;
            if reset_rule.recursive {
                return trigger_metadata_only_hard_compaction_flow_for_own_downstream_datasets(
                    self.dataset_entry_service.as_ref(),
                    self.dependency_graph_service.as_ref(),
                    self.flow_run_service.as_ref(),
                    &dataset_id,
                    activation_cause,
                )
                .await;
            }
        }

        // No propagation needed
        Ok(())
    }
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
